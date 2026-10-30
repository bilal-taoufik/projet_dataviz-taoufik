"""
Outils partagés par les notebooks du cours.

    import sys; sys.path.append("/home/jovyan/work")
    from commun.spark_utils import creer_session, LAKE, RAW

    spark = creer_session("S02")              # Spark + Delta Lake + data lake S3 (RustFS) + PostgreSQL
    spark = creer_session("S01", lac=False)   # Spark seul, sans data lake

La première session télécharge les jars nécessaires (Delta, S3, JDBC).
Ils sont gardés dans un volume Docker : les sessions suivantes démarrent vite.

Important : appeler creer_session() AVANT toute autre utilisation de Spark dans le notebook.
Pour changer d'options, redémarrer le noyau (Kernel → Restart).
"""
import os
import subprocess
import sys

from pyspark.sql import SparkSession

# --- Chemins -----------------------------------------------------------------
WORK = "/home/jovyan/work"
RAW = f"{WORK}/data/raw"                 # fichiers téléchargés (sur le disque)
BUCKET = "datalake"
LAKE = f"s3a://{BUCKET}"                 # data lake : s3a://datalake/bronze/..., /silver/..., /gold/...

# --- Connexions (fournies par compose.yaml) ----------------------------------
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://rustfs:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "rustfsadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "rustfsadmin")
PG_URL = os.getenv("PG_URL", "jdbc:postgresql://postgres:5432/nyctaxi")
PG_PROPS = {
    "user": os.getenv("PG_USER", "cours"),
    "password": os.getenv("PG_PASSWORD", "cours"),
    "driver": "org.postgresql.Driver",
}

# Version de Delta Lake compatible avec chaque version de Spark (major.minor)
# Delta 4.4.1 est construit pour Spark 4.0, 4.1 et 4.2 (artefact suffixé par la version de Spark).
_DELTA_POUR_SPARK = {"3.5": "3.3.0", "4.0": "4.4.1", "4.1": "4.4.1", "4.2": "4.4.1"}
_POSTGRES_JDBC = "org.postgresql:postgresql:42.7.4"


def _sonder_versions():
    """Lit les versions de Spark, Scala et Hadoop SANS démarrer Spark.

    (Les jars supplémentaires ne sont chargés qu'au tout premier démarrage de Spark
    dans le notebook : il faut donc connaître les versions avant.)
    """
    import glob
    import re
    import pyspark

    home = os.getenv("SPARK_HOME") or os.path.dirname(pyspark.__file__)
    jars = [os.path.basename(j) for j in glob.glob(os.path.join(home, "jars", "*.jar"))]

    def chercher(motif, defaut):
        for j in jars:
            m = re.match(motif, j)
            if m:
                return m.group(1)
        return defaut

    return {
        "spark": pyspark.__version__,
        "scala": chercher(r"scala-library-(\d+\.\d+\.\d+)\.jar", "2.13.0"),
        "hadoop": chercher(r"hadoop-client-api-(\d+\.\d+\.\d+)\.jar", "3.4.1"),
    }


def _installer_delta_python(version):
    """Installe le module Python delta-spark sans toucher au PySpark de l'image."""
    try:
        import delta  # noqa: F401
        return True
    except ImportError:
        r = subprocess.run([sys.executable, "-m", "pip", "install", "-q", "--no-deps",
                            f"delta-spark=={version}"], capture_output=True, text=True)
        return r.returncode == 0


def creer_session(nom="cours", lac=True, delta=True, postgres=True, memoire="4g"):
    """Crée une session Spark locale, branchée sur le data lake S3 et PostgreSQL."""
    v = _sonder_versions()
    spark_mm = ".".join(v["spark"].split(".")[:2])
    scala_bin = ".".join(v["scala"].split(".")[:2])

    paquets = []
    if lac:
        paquets.append(f"org.apache.hadoop:hadoop-aws:{v['hadoop']}")
    if postgres:
        paquets.append(_POSTGRES_JDBC)

    delta_ok = False
    if delta:
        dv = _DELTA_POUR_SPARK.get(spark_mm)
        if dv and _installer_delta_python(dv):
            if spark_mm.startswith("3."):
                artefact = f"io.delta:delta-spark_{scala_bin}:{dv}"
            else:  # Delta 4.x : delta-spark_<spark>_<scala>
                artefact = f"io.delta:delta-spark_{spark_mm}_{scala_bin}:{dv}"
            paquets.append(artefact)
            delta_ok = True
        else:
            print(f"⚠ Delta Lake indisponible pour Spark {v['spark']} : les tables seront écrites en Parquet. "
                  "Les démonstrations Delta (historique, time travel) seront sautées.")

    b = (SparkSession.builder
         .appName(nom)
         .master("local[*]")
         .config("spark.driver.memory", memoire)
         .config("spark.sql.session.timeZone", "America/New_York"))
    if paquets:
        b = b.config("spark.jars.packages", ",".join(paquets))
    if delta_ok:
        b = (b.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
              .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    if lac:
        b = (b.config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
              .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
              .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
              .config("spark.hadoop.fs.s3a.path.style.access", "true")
              .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
              .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
              .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"))

    print(f"Préparation de Spark {v['spark']} (Hadoop {v['hadoop']})"
          + (" + Delta Lake" if delta_ok else "") + (" + data lake S3" if lac else "")
          + " … le premier démarrage peut prendre quelques minutes.")
    spark = b.getOrCreate()
    spark.conf.set("cours.format_tables", "delta" if delta_ok else "parquet")
    print("Session prête. Spark UI : http://localhost:4040")
    return spark


def format_tables(spark):
    """'delta' si Delta Lake est actif, sinon 'parquet'."""
    return spark.conf.get("cours.format_tables", "parquet")


def creer_bucket(nom=BUCKET):
    """Crée le bucket du data lake s'il n'existe pas (alternative à la console RustFS)."""
    try:
        import boto3
    except ImportError:
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "boto3"], check=True)
        import boto3
    s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_ACCESS_KEY,
                      aws_secret_access_key=S3_SECRET_KEY, region_name="us-east-1")
    existants = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if nom not in existants:
        s3.create_bucket(Bucket=nom)
        print(f"Bucket « {nom} » créé.")
    else:
        print(f"Bucket « {nom} » déjà présent.")


def vers_postgres(df, table, mode="overwrite"):
    """Copie un DataFrame Spark dans une table PostgreSQL (couche de restitution)."""
    df.write.jdbc(PG_URL, table, mode=mode, properties=PG_PROPS)
    print(f"Table PostgreSQL « {table} » écrite.")