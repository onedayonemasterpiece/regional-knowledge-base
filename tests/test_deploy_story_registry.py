"""Source-only deployment regressions; every service command is mocked."""
import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/production/deploy_story_registry.py"
spec = importlib.util.spec_from_file_location("deploy_story_registry_tested", SCRIPT)
deploy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy)


class ReleaseConfigurationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.source = self.root / "candidate/source"
        (self.source / "src/regional_knowledge").mkdir(parents=True)
        (self.source / "src/regional_knowledge/story_ingestion.py").write_text("# fixture\n")
        self.sha = "c" * 40
        self.patch = mock.patch.multiple(
            deploy, UNIT_DIR=self.root / "units", STATE=self.root / "state",
            ENV=self.root / "state/service.env",
            OVERRIDE_ENV=self.root / "state/story-registry-release.env",
            ARCHIVE_ENV=self.root / "state/archive-s2s-pythonpath.env",
            OVERRIDE=self.root / "units/api/90-story-registry.conf",
            INDEXER_OVERRIDE=self.root / "units/indexer/90-story-ingestion.conf",
            GRAPH_OVERRIDE=self.root / "units/graph/90-model-led-locations.conf")
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.write(deploy.ENV, "SECRET_BINDING=unchanged-fixture\n", 0o600)
        self.write(deploy.OVERRIDE_ENV,
                   '# retained\nPYTHONPATH="/old/api/src"\nRKB_RELEASE_SHA="old"\nOTHER=keep\n', 0o600)
        self.write(deploy.ARCHIVE_ENV, "PYTHONPATH=/old/indexer/src\nM2M_CONFIG=keep\n", 0o600)
        for path in (deploy.OVERRIDE, deploy.INDEXER_OVERRIDE):
            self.write(path, "[Service]\nWorkingDirectory=/old/source\nEnvironmentFile=\n"
                       "EnvironmentFile=service.env\nEnvironmentFile=story-registry-release.env\n"
                       "EnvironmentFile=bridge.env\n", 0o644)
        self.late = []
        for unit in (deploy.SERVICE, deploy.INDEXER_SERVICE):
            path = deploy.UNIT_DIR / (unit + ".d") / deploy.LATE_PIN_NAME
            self.late.append(path)
            extra = "EnvironmentFile=archive-s2s-pythonpath.env\n" if unit == deploy.INDEXER_SERVICE else ""
            self.write(path, "[Service]\nWorkingDirectory=/old/source\n"
                       "Environment=PYTHONPATH=/old/source/src\n" + extra, 0o644)

    def write(self, path, text, mode):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        path.chmod(mode)

    def plan(self):
        return deploy.configuration_plan(self.source, self.sha, with_indexer=True)

    def test_both_existing_late_pins_and_envfiles_follow_one_candidate(self):
        before = {p: p.read_bytes() for p in [deploy.ENV, deploy.OVERRIDE_ENV, deploy.ARCHIVE_ENV, *self.late]}
        plan = self.plan()
        self.assertNotIn(deploy.ENV, plan)
        self.assertNotIn(deploy.GRAPH_OVERRIDE, plan)
        self.assertEqual(len(plan), 6)
        changed = []
        deploy.configure(plan, changed)
        for path in (deploy.OVERRIDE_ENV, deploy.ARCHIVE_ENV):
            self.assertIn('PYTHONPATH="' + str(self.source / "src") + '"', path.read_text())
            self.assertIn('RKB_RELEASE_SHA="' + self.sha + '"', path.read_text())
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        for path in self.late:
            self.assertIn("WorkingDirectory=" + str(self.source), path.read_text())
            self.assertIn("Environment=PYTHONPATH=" + str(self.source / "src"), path.read_text())
            self.assertEqual(path.stat().st_mode & 0o777, 0o644)
        self.assertIn("M2M_CONFIG=keep\n", deploy.ARCHIVE_ENV.read_text())
        self.assertIn("OTHER=keep\n", deploy.OVERRIDE_ENV.read_text())
        self.assertIn("EnvironmentFile=archive-s2s-pythonpath.env\n", self.late[1].read_text())
        self.assertIn("EnvironmentFile=bridge.env\n", deploy.OVERRIDE.read_text())
        self.assertEqual(deploy.ENV.read_bytes(), before[deploy.ENV])
        # Existing late EnvironmentFile used to override the release value.
        merged = {}
        for path in (deploy.OVERRIDE_ENV, deploy.ARCHIVE_ENV):
            for line in path.read_text().splitlines():
                if "=" in line and not line.startswith("#"):
                    key, value = line.split("=", 1)
                    merged[key] = value.strip('"')
        self.assertEqual(merged["PYTHONPATH"], str(self.source / "src"))
        deploy.restore_configuration(plan, changed)
        for path, original in before.items():
            self.assertEqual(path.read_bytes(), original)

    def test_api_only_leaves_indexer_and_archive_configuration_untouched(self):
        plan = deploy.configuration_plan(self.source, self.sha)
        self.assertNotIn(deploy.ARCHIVE_ENV, plan)
        self.assertNotIn(deploy.INDEXER_OVERRIDE, plan)
        self.assertNotIn(self.late[1], plan)

    def test_absent_late_files_are_not_created(self):
        for path in [deploy.ARCHIVE_ENV, *self.late]:
            path.unlink()
        plan = self.plan()
        changed = []
        deploy.configure(plan, changed)
        for path in [deploy.ARCHIVE_ENV, *self.late]:
            self.assertFalse(path.exists())

    def test_snapshot_conflict_stops_before_any_write(self):
        plan = self.plan()
        deploy.ARCHIVE_ENV.write_text("PYTHONPATH=/another/operator\n")
        changed = []
        with self.assertRaisesRegex(RuntimeError, "changed after preflight"):
            deploy.configure(plan, changed)
        self.assertEqual(changed, [])
        self.assertEqual(deploy.OVERRIDE_ENV.read_bytes(), plan[deploy.OVERRIDE_ENV][0])

    def test_partial_configuration_failure_restores_all_written_bytes(self):
        plan = self.plan()
        changed = []
        real = deploy.atomically
        def fail(path, *args, **kwargs):
            if path == deploy.ARCHIVE_ENV:
                raise OSError("injected last-file failure")
            return real(path, *args, **kwargs)
        with mock.patch.object(deploy, "atomically", side_effect=fail):
            with self.assertRaises(OSError):
                deploy.configure(plan, changed)
        self.assertTrue(changed)
        deploy.restore_configuration(plan, changed)
        for path, (previous, _, _) in plan.items():
            self.assertEqual(path.read_bytes(), previous)

    def test_rollback_refuses_to_overwrite_concurrent_operator_change(self):
        plan = self.plan()
        changed = []
        deploy.configure(plan, changed)
        deploy.ARCHIVE_ENV.write_text("PYTHONPATH=/another/operator\n")
        with self.assertRaisesRegex(RuntimeError, "concurrent configuration"):
            deploy.restore_configuration(plan, changed)
        self.assertEqual(deploy.ARCHIVE_ENV.read_text(), "PYTHONPATH=/another/operator\n")
        self.assertEqual(deploy.OVERRIDE_ENV.read_bytes(), plan[deploy.OVERRIDE_ENV][0])

    def test_duplicate_release_keys_and_mixed_environment_fail_closed(self):
        with self.assertRaisesRegex(RuntimeError, "ambiguous"):
            deploy.release_environment(b"PYTHONPATH=a\nPYTHONPATH=b\n", self.source, self.sha)
        with self.assertRaisesRegex(RuntimeError, "mixed late"):
            deploy.retarget_override(
                b"[Service]\nWorkingDirectory=/old\nEnvironment=PYTHONPATH=/old M2M=preserve\n",
                self.source, late=True)

    def test_failed_pid_verification_rolls_back_exact_all_configs(self):
        originals = {p: old for p, (old, _, _) in self.plan().items()}
        with mock.patch.object(deploy, "materialize", return_value=self.source), \
             mock.patch.object(deploy, "verify_imports"), \
             mock.patch.object(deploy, "offline_backup", return_value={"db_backup": "mock-only"}), \
             mock.patch.object(deploy, "systemctl") as systemctl, \
             mock.patch.object(deploy, "cmd", return_value=str(self.source)), \
             mock.patch.object(deploy, "health"), \
             mock.patch.object(deploy, "verify_pid", side_effect=RuntimeError("indexer mismatch")):
            with self.assertRaisesRegex(RuntimeError, "indexer mismatch"):
                deploy.run(self.sha, True, with_indexer=True)
        for path, original in originals.items():
            self.assertEqual(path.read_bytes(), original)
        self.assertEqual(systemctl.call_args_list.count(mock.call("restart", deploy.INDEXER_SERVICE)), 2)

    def test_effective_cwd_mismatch_rolls_back_without_restarting(self):
        originals = {p: old for p, (old, _, _) in self.plan().items()}
        with mock.patch.object(deploy, "materialize", return_value=self.source), \
             mock.patch.object(deploy, "verify_imports"), \
             mock.patch.object(deploy, "offline_backup", return_value={"db_backup": "mock-only"}), \
             mock.patch.object(deploy, "systemctl") as systemctl, \
             mock.patch.object(deploy, "cmd", return_value="/unhandled/late/pin"), \
             mock.patch.object(deploy, "health"):
            with self.assertRaisesRegex(RuntimeError, "effective working directory"):
                deploy.run(self.sha, True, with_indexer=True)
        self.assertFalse(any(call.args[0] == "restart" for call in systemctl.call_args_list))
        for path, original in originals.items():
            self.assertEqual(path.read_bytes(), original)

    def test_verify_pid_requires_actual_and_configured_cwd_plus_environment(self):
        fake_proc = mock.Mock()
        fake_process = mock.Mock()
        fake_proc.__truediv__ = mock.Mock(return_value=fake_process)
        fake_process.joinpath.return_value.resolve.return_value = self.source
        environment = mock.Mock()
        fake_process.__truediv__ = mock.Mock(return_value=environment)
        environment.read_bytes.return_value = (
            "PYTHONPATH=" + str(self.source / "src") + "\0RKB_RELEASE_SHA=" + self.sha + "\0").encode()
        with mock.patch.object(deploy, "Path", return_value=fake_proc), \
             mock.patch.object(deploy, "cmd", side_effect=["123", str(self.source)]):
            self.assertEqual(deploy.verify_pid(self.source, self.sha), 123)
        fake_process.joinpath.return_value.resolve.return_value = self.root / "old"
        with mock.patch.object(deploy, "Path", return_value=fake_proc), \
             mock.patch.object(deploy, "cmd", side_effect=["123", str(self.source)]):
            with self.assertRaisesRegex(RuntimeError, "working directory"):
                deploy.verify_pid(self.source, self.sha)


if __name__ == "__main__":
    unittest.main()
