# Sources

Extracted owner code provenance: [EXTRACTION_PROVENANCE.json](EXTRACTION_PROVENANCE.json).
Compiler evidence alone does not prove behavioral parity or exchange acceptance.

Official Binance sources reviewed October15,2026:

- [Spot market endpoints](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints).
- [Spot WebSocket streams](https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams).

Existing USD-M parser is the narrow public aggregate-trade/depth profile inherited
from the source project, including `nq`, `st` and product identity requirements.
Other products/legacy lanes need their own reviewed profile; do not relax missing
fields implicitly. Symbol syntax validation is not exchangeInfo listing validation.
