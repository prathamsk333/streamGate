# Custom rule types

A rule type is two classes: a pydantic spec that validates the YAML, and a rule that does the
check. Publish the spec under the `streamgate.rules` entry point and StreamGate picks it up.

```python
# my_rules.py
from collections.abc import Mapping
from typing import Any, ClassVar

from streamgate.rules.base import Context, Rule, Severity
from streamgate.rules.fields import MISSING, FieldPath
from streamgate.rules.spec import FieldName, RuleSpec


class PincodeSpec(RuleSpec):
    kind: ClassVar[str] = "pincode"
    field: FieldName

    def build(self, severity: Severity) -> Rule:
        return PincodeRule(self, severity)


class PincodeRule(Rule):
    def __init__(self, spec: PincodeSpec, severity: Severity) -> None:
        super().__init__(spec.id, severity, spec.message)
        self.field = FieldPath(spec.field)

    def check(self, data: Mapping[str, Any], ctx: Context) -> str | None:
        value = self.field.get(data)
        if value is MISSING or value is None:
            return None
        if isinstance(value, str) and len(value) == 6 and value.isdigit() and value[0] != "0":
            return None
        return f"{self.field} is not a valid PIN code"
```

```toml
# pyproject.toml of your package
[project.entry-points."streamgate.rules"]
pincode = "my_rules:PincodeSpec"
```

Then in a rule file:

```yaml
- {id: address.pin, type: pincode, field: address.pin}
```

Notes:

- `check` returns `None` to pass, or a message. It must not mutate `data`.
- Exceptions raised by `check` are caught and turned into a violation, so a bug in a plugin
  quarantines records instead of crashing the gateway. Still, don't rely on it.
- Spec validation errors (raise `ValueError` in a pydantic validator) are reported by
  `streamgate rules validate` with the rule's position and id.
- Subclass `StatefulRule` if the rule keeps state across records (it will run last and be
  skipped for rejected records), or `MaskingRule` if it rewrites values.
