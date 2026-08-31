class DagvoraError(Exception):
    pass


class DuplicateTaskError(DagvoraError, ValueError):
    pass


class MissingTaskError(DagvoraError, ValueError):
    pass


class CycleError(DagvoraError, ValueError):
    pass
