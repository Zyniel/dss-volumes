"""Use case of a DSS project.

Every part of dbmon asks this module, so the rule changes in one place:
- target rule: the project's tag in the "CDP-UseCase" global tag category. DSS lists such a
  tag among the project's tags as "CDP-UseCase:<use case>".
- until every project carries that tag, the current criteria apply (_current_rule).
"""

TAG_CATEGORY = "CDP-UseCase"


def project_use_case(project_key, tags):
    """Use case of a project, or None when it cannot be told.

    project_key  DSS project key
    tags         the project's tags, as listed by the DSS API
    """
    return _from_tag(tags) or _current_rule(project_key, tags)


def _from_tag(tags):
    """Value of the CDP-UseCase tag. Several values come back comma separated, so that
    the inconsistency shows in the reports."""
    prefix = TAG_CATEGORY.lower() + ":"
    values = sorted({tag.split(":", 1)[1].strip() for tag in tags or [] if tag.lower().startswith(prefix)})
    return ",".join(values) or None


def _current_rule(project_key, tags):
    """Current criteria, used while projects are not tagged yet. To be filled in.
    Remove once every project carries a CDP-UseCase tag."""
    return None
