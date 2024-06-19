import datetime


def brasilia_timezone() -> datetime.timezone:
    return datetime.timezone(datetime.timedelta(hours=-3), "BRT")


def now(timezone: datetime.timezone) -> datetime.datetime:
    return datetime.datetime.now(timezone)


def now_str(timezone) -> str:
    return now(timezone).strftime("%d/%m/%Y %H:%M")


def now_brasilia_str() -> str:
    return now_str(brasilia_timezone())


def now_brasilia_str_report() -> str:
    report_time = now_brasilia_str().replace(" ", "\\A ")
    return report_time
