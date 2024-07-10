from src import app
from src.utils import my_time

if __name__ == "__main__":
    report_time = my_time.now_brasilia_str_report()

    app.main()
    app.create_enade_report(report_time)
