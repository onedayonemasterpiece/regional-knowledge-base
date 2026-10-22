# Exact wheel references in the launch descriptor

The complete55-wheel/103-native/schema14 metadata fixture reproduced a67410-byte
launch configuration against the existing65536-byte limit. The55 wheel names
alone occupy3454 bytes and repeat the exact names already pinned in downloads.

Stage0Spec now emits selected wheel download indices when every selected wheel
is present in downloads. Names/order, package versions and all size/hash pins
are reconstructed before preparation; downloaded but unselected wheels never
become install inputs. Legacy name lists remain accepted and cached-only wheel
names keep that representation. `wire(compact_wheels=False)` explicitly emits
the legacy shape. Index lists have the same128 count bound and reject booleans,
duplicates, extra fields and out-of-range indices. The existing runtime decoder
must accompany this descriptor in the next notebook; no live hot patch occurs.

This changes no count or byte budget and no target/launch composition code.
PR242 separately raises complete prepared-file count to192. Full source and
actual package metadata are verified in the existing combined candidate CI;
fixture identities/GET signatures/journal are synthetic, not live adoption.
The full bd138b2 v4.1, owner alignment and acceptance contracts remain in force.
