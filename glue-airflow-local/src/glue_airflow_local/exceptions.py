"""Exception hierarchy.

GlueParseError covers Terraform-to-IR parsing failures (unresolved references,
malformed blocks). InvalidWorkflowError covers IR invariant violations
(structural issues caught after parsing). UnsupportedTriggerError flags Glue
features outside the current supported scope.
"""


class GlueAirflowLocalError(Exception):
    """Base class for all errors raised by this package."""


class GlueParseError(GlueAirflowLocalError):
    """Raised when Terraform input cannot be parsed into the IR."""


class InvalidWorkflowError(GlueAirflowLocalError):
    """Raised when an IR object violates a structural invariant."""


class UnsupportedTriggerError(GlueAirflowLocalError):
    """Raised when a Glue trigger type is recognised but not supported in this version."""


class UnresolvedReferenceError(GlueParseError):
    """Raised when a Terraform reference cannot be resolved."""
