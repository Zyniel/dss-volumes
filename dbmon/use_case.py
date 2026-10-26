"""Use case of a DSS project.

Every part of dbmon asks this module, so the rule changes in one place:
- target rule: the project's tag in the "CDP-UseCase" global tag category. DSS lists such
  a tag among the project's tags as "CDP-UseCase:<use case>".
- until every project carries that tag, the current criteria apply (current_rule).
"""

TAG_CATEGORY = "CDP-UseCase"


def project_use_case(project_key, tags):
    """Use case of a project, or None when it cannot be told.

    project_key  DSS project key
    tags         the project's tags, as listed by the DSS API
    """
    use_case, source = project_use_case_and_source(project_key, tags)
    return use_case


def project_use_case_and_source(project_key, tags):
    """(use case, source) of a project. source: TAG (CDP-UseCase tag), RULE (current
    criteria) or NONE (use case unknown)."""
    use_case = use_case_from_tag(tags)
    if use_case is not None:
        return use_case, "TAG"

    use_case = current_rule(project_key, tags)
    if use_case is not None:
        return use_case, "RULE"

    return None, "NONE"


def use_case_from_tag(tags):
    """Value of the CDP-UseCase tag, or None. When a project has several such tags, they
    are returned comma separated, so that the inconsistency shows in the reports."""
    if tags is None:
        return None

    prefix = TAG_CATEGORY.lower() + ":"
    values = set()
    for tag in tags:
        if not tag.lower().startswith(prefix):
            continue
        value = tag[len(prefix):].strip()
        if value != "":
            values.add(value)

    if not values:
        return None
    return ",".join(sorted(values))


def current_rule(project_key, tags):
    """Current criteria, used while projects are not tagged yet. To be filled in.
    Remove once every project carries a CDP-UseCase tag."""
    return None
