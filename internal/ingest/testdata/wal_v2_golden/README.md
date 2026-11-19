# WAL v2.0 golden fixtures

These immutable files freeze the v2.0 byte contract independently of the current writer and reader. `TestWALV2GoldenFixtures` compares the encoder output byte-for-byte and decodes the fixed fixtures. Do not regenerate them as part of routine development. A format change requires an explicit version/compatibility decision, a migration policy, and reviewed fixture updates.

Files:

- `header-v2.bin` — one segment header, including the envelope schema and both descriptor digests.
- `frame-v2.bin` — one record frame with unknown protobuf wire fields in the raw payload.
- `active-segment-v2.bin` — header plus the one committed frame.
- `sealed-segment-v2.bin` — active segment bytes plus the sealed footer.
- `next-active-header-v2.bin` — empty active segment header after rotation.
- `manifest-v2.json` and `current-v2.txt` — generation 2 manifest and its CURRENT pointer.

SHA-256 (fixture bytes):

```text
11e2beddf2759c127322f3f7a1ed6a19a13471f7f6fde022df7d461ec10fbe3e  active-segment-v2.bin
53c234e5e8472b6ac51c1ae1cab3fe06fad053beb8ebfd8977b010655bfdd3c3  current-v2.txt
f54b92d2d1e8050b088a93575cba4eff15ca48a84b862b08fbfe21c9aff669ad  frame-v2.bin
eb503a6e7dd97e45bd60d63abea63ddbec5c0a2bb72bcf5f532bbc905256c965  header-v2.bin
c8f30369da59b074d0c17716e0b0327e845ee0abc0fe7d689fe69c7da9478ae6  manifest-v2.json
a924293b5e7693366e87cea04ed4f8e8a892283685a2357748a5d61793c76091  next-active-header-v2.bin
7a83238cf6309ec5a272248a285d0b0aee880fa825c602700b041a20ea4ac688  sealed-segment-v2.bin
```
