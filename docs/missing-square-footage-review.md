# Missing square-footage investigation

Checked September 15, 2026 against the configured application database and the
Wayne County Auditor public ArcGIS parcel layer. The user explicitly authorized
transmitting the listing addresses to that public service for this lookup.

The current shadow population contains nine scoring-eligible listings with no
provider square footage. Stored listing payloads have no additional original
area field, and exact matching against locally stored comparable sales found no
records for these addresses.

The auditor lookup returned exactly one address match for every listing. All
nine candidates have one dwelling and a residential class. Two required address
normalization: `St` to the auditor's `Ave`, and `Applecreek` to `Apple Creek`.

Living area ranges from 1,040 to 1,944 square feet, and construction years range
from 1920 to 2024. Eight candidates use residential class 510 and one uses class
511. The exact listing-to-parcel mapping is retained locally under the ignored
`data` directory and is excluded from version control.

These values are candidate evidence and have not been written to listings or
used in valuations. Address agreement, one-dwelling status, and residential
class make the matches plausible, but an address match alone is not a durable
provenance model.

The next implementation should store property-attribute evidence separately,
including listing identity, parcel, field name, value, source URL, retrieval
time, matching method, and review status. Scoring should consume an approved
effective value without overwriting the provider's original null. A later
provider sync must not erase an approved enrichment, and changed auditor values
must create new auditable evidence rather than mutate history.

Source: `https://services6.arcgis.com/WiOy9S7NUTWyXUe4/arcgis/rest/services/parcel_joined/FeatureServer/0/query`
