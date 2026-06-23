import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "local_superset_secret_key_for_fpp")
SQLALCHEMY_DATABASE_URI = os.environ.get("SUPERSET_DATABASE_URI")
WTF_CSRF_ENABLED = True
TALISMAN_ENABLED = False
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}
