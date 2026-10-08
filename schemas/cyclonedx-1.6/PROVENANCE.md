# Fixed offline schema closure

These four JSON documents are retained verbatim from
[Goneat commit 2aea6eacad7dcb7d93172a427e0d37a2cfcf590d](https://github.com/fulmenhq/goneat/tree/2aea6eacad7dcb7d93172a427e0d37a2cfcf590d/schemas).
The tracked manifest binds their exact hashes and source paths. References must
resolve inside this closure; validation also uses operating-system network denial.

The CycloneDX 1.6 schema, SPDX identifier enumeration and JSF schema come from
[CycloneDX specification tag 1.7](https://github.com/CycloneDX/specification/tree/1.7/schema)
and retain its Apache-2.0 license in CycloneDX-LICENSE.txt. The separate SPDX 2.3
schema is not part of this closure.

The draft-07 meta-schema matches the parsed published schema at JSON Schema
Specification commit 567f768506aaa33a38e552c85bf0586029ef1b32. Its historical
repository README grants BSD or AFL. JSON-Schema-LICENSE.txt preserves the complete
[upstream notice at commit 51326f80900357fe3069beb4f5f575db24c1b9a7](https://github.com/json-schema-org/json-schema-spec/blob/51326f80900357fe3069beb4f5f575db24c1b9a7/LICENSE),
which clarifies BSD-3-Clause OR AFL-3.0 and carries Copyright (c) 2022 JSON Schema
Specification Authors. The component expression remains BSD-3-Clause OR AFL-3.0. This distribution
exercises the BSD-3-Clause grant and preserves both
complete upstream texts. The reviewed meta-schema formatting and bytes remain
unchanged; the Goneat wrapper license is not substituted for the upstream grant.
