"""Pure exact-pin and accepted-byte tests, no provider or PG."""
from dataclasses import replace
import unittest
from regional_knowledge.kaggle_native_source_upgrade import (
    NativeSourceUpgradePins,native_source_upgrade_handler,resolved_native_source_upgrade,
    validate_native_source_upgrade,canonical,sha,transition_sql)
from regional_knowledge.geo_manifest_commands import GEO_V2_REVISIONS,GEO_DEPENDENCY_MIGRATION
from regional_knowledge.source_activation import ACTIVATION_REVISIONS


class UpgradeContractTests(unittest.TestCase):
    def pins(self):return NativeSourceUpgradePins('a'*64,'b'*64,'c'*64,'d'*64)
    def command(self,pins):
        return resolved_native_source_upgrade(pins=pins,sequence=26,operation_key='native-upgrade',
            request_id='native-upgrade-request',person_id='00000000-0000-0000-0000-000000000001',
            logical_time='2026-10-21T00:00:00Z')
    def test_exact12_to14_transition_and_both_ordered_artifact_pins(self):
        pins=self.pins();handler=native_source_upgrade_handler(pins);command=self.command(pins)
        self.assertEqual(handler.revisions,GEO_V2_REVISIONS)
        self.assertEqual(handler.transition.target,ACTIVATION_REVISIONS)
        self.assertEqual(handler.transition.predecessor,GEO_DEPENDENCY_MIGRATION)
        self.assertEqual(handler.transition.artifact_sha256,sha(canonical(pins.manifest)))
        self.assertTrue(handler.transition.preserve_read_frontier)
        self.assertEqual([a['name'] for a in pins.manifest['artifacts']],['013_source_stage.sql','014_source_stage_activation.sql'])
        before=command.encode();validate_native_source_upgrade(command,pins=pins)
        self.assertEqual(command.encode(),before)
    def test_changed_either_artifact_or_actor_or_revision_rejected(self):
        pins=self.pins();command=self.command(pins)
        for changed in (replace(pins,sql013_sha256='e'*64),replace(pins,sql014_sha256='e'*64)):
            with self.assertRaises(ValueError):validate_native_source_upgrade(command,pins=changed)
        for changed in (replace(command,person_id='00000000-0000-0000-0000-000000000002'),
                        replace(command,schema_revision=ACTIVATION_REVISIONS.schema),
                        replace(command,kind='domain_command')):
            with self.assertRaises(ValueError):validate_native_source_upgrade(changed,pins=pins)
    def test_outcome_definition_retains_both_artifact_hashes(self):
        sql=transition_sql(self.pins())
        self.assertIn('a'*64,sql);self.assertIn('b'*64,sql)
        self.assertNotIn('CREATE OR REPLACE',sql)
