from airflow.models import Variable

def get_watermark(name, default_iso):
    return Variable.get(name, default_var=default_iso)

def set_watermark(name, iso):
    Variable.set(name, iso)