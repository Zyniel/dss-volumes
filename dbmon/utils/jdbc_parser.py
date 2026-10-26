"""STUB. Stands in for the jdbc_parser library of the professional environment, which replaces
this file. Keep the same interface: parse_jdbc(url) returns a JdbcInfo.

This stand-in only knows the common Oracle thin and PostgreSQL URL forms, enough for the
tests:
    jdbc:oracle:thin:@//host:port/service
    jdbc:oracle:thin:@host:port/service
    jdbc:oracle:thin:@host:port:SID
    jdbc:oracle:thin:user/password@//host:port/service   (the password is never kept)
    jdbc:postgresql://host:port/database?user=name
Anything else gives a JdbcInfo with the vendor only.
"""
import re
from dataclasses import dataclass
from typing import Optional

ORACLE_THIN = re.compile(r"^jdbc:oracle:thin:(?:(?P<user>[^/@]+)(?:/[^@]*)?)?@(?://)?(?P<host>[^:/?]+)(?::(?P<port>\d+))?[/:](?P<database>[^?;]+)")
POSTGRESQL = re.compile(r"^jdbc:postgresql://(?P<host>[^:/?]+)(?::(?P<port>\d+))?/(?P<database>[^?;]+)")
USER_PARAMETER = re.compile(r"[?&;]user=(?P<user>[^&;]+)")


@dataclass
class JdbcInfo:
    vendor: str
    host: Optional[str]
    port: Optional[str]
    database: Optional[str]
    user: Optional[str]
    raw: str


def parse_jdbc(url):
    """JdbcInfo of a JDBC connection string."""
    text = url.strip()
    vendor = "unknown"
    if text.lower().startswith("jdbc:"):
        vendor = text.split(":")[1].lower()

    match = None
    if vendor == "oracle":
        match = ORACLE_THIN.match(text)
    elif vendor == "postgresql":
        match = POSTGRESQL.match(text)

    if match is None:
        return JdbcInfo(vendor=vendor, host=None, port=None, database=None, user=None, raw=text)

    user = match.groupdict().get("user")
    user_match = USER_PARAMETER.search(text)
    if user is None and user_match is not None:
        user = user_match.group("user")

    return JdbcInfo(
        vendor=vendor,
        host=match.group("host"),
        port=match.group("port"),
        database=match.group("database"),
        user=user,
        raw=text,
    )
