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

    def test_rotation_updates_both_existing_pins_before_exec_without_stopping_runtime(self):
        controller = "regional-knowledge-bge-controller.service"
        current = self.root / "installed/source"
        old_api = self.root / "older-api/source"
        for source, sha in ((current, "a" * 40), (old_api, "b" * 40)):
            self.write(source / ".rkb-release-sha", sha, 0o600)
        api_drop = deploy.UNIT_DIR / (deploy.SERVICE + ".d/zzzzzzzzzzzzzzz-kaggle-source-acceptance.conf")
        ctrl_drop = deploy.UNIT_DIR / (controller + ".d/90-kaggle-bootstrap.conf")
        self.write(api_drop, "[Service]\nWorkingDirectory=" + str(old_api)
                   + "\nEnvironmentFile=" + str(deploy.OVERRIDE_ENV) + "\n", 0o600)
        self.write(ctrl_drop, "[Service]\nWorkingDirectory=" + str(current)
                   + "\nEnvironment=PYTHONPATH=" + str(current / "src")
                   + "\nEnvironment=RKB_RELEASE_SHA=" + "a" * 40
                   + "\nExecStart=/usr/bin/python3 " + str(current / "scripts/production/bge_kaggle_controller.py")
                   + "\nEnvironmentFile=private-template.env\n", 0o600)
        before = {p: p.read_bytes() for p in (api_drop, ctrl_drop, deploy.OVERRIDE_ENV)}
        class Exec(BaseException): pass
        commands = []
        def command(*args, **kwargs):
            commands.append(args)
            return self.sha if "rev-parse" in args else ""
        with mock.patch.object(deploy, "__file__", str(current / "scripts/production/deploy_story_registry.py")), \
             mock.patch.object(deploy, "running_source", side_effect=lambda unit: old_api if unit == deploy.SERVICE else current), \
             mock.patch.object(deploy, "cmd", side_effect=command), \
             mock.patch.object(deploy, "materialize", return_value=self.source), \
             mock.patch.object(deploy, "compiler_checkpoint_preflight") as preflight, \
             mock.patch.object(deploy.os, "chdir"), \
             mock.patch.object(deploy.os, "execve", side_effect=Exec) as execute:
            # A compiler/Saved failure must leave configuration untouched.
            preflight.side_effect = RuntimeError("checkpoint required")
            deploy.refresh_main_for_rotation()
            for path, raw in before.items(): self.assertEqual(path.read_bytes(), raw)
            execute.assert_not_called()
            preflight.side_effect = None
            write = deploy.atomically
            def fail_controller(path, *args, **kwargs):
                if path == ctrl_drop: raise OSError("synthetic write failure")
                return write(path, *args, **kwargs)
            with mock.patch.object(deploy, "atomically", side_effect=fail_controller):
                deploy.refresh_main_for_rotation()
            for path, raw in before.items(): self.assertEqual(path.read_bytes(), raw)
            execute.assert_not_called()
            with self.assertRaises(Exec): deploy.refresh_main_for_rotation()
        self.assertIn(str(self.source), api_drop.read_text())
        self.assertIn(str(self.source / "scripts/production/bge_kaggle_controller.py"), ctrl_drop.read_text())
        self.assertIn("EnvironmentFile=private-template.env", ctrl_drop.read_text())
        self.assertIn("Environment=RKB_RELEASE_SHA=" + self.sha, ctrl_drop.read_text())
        self.assertIn("OTHER=keep", deploy.OVERRIDE_ENV.read_text())
        self.assertEqual(deploy.OVERRIDE_ENV.stat().st_mode & 0o777, 0o600)
        self.assertEqual(execute.call_args.args[2]["RKB_RELEASE_SHA"], self.sha)
        for sha in ("a" * 40, "b" * 40):
            self.assertIn(("git", "-C", deploy.ROOT, "merge-base", "--is-ancestor", sha, self.sha), commands)
        self.assertEqual([c for c in commands if c[0] == "systemctl"],
                         [("systemctl", "--user", "daemon-reload")] * 2)

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
             mock.patch.object(deploy, "compiler_checkpoint_preflight"), \
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
             mock.patch.object(deploy, "compiler_checkpoint_preflight"), \
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


class CompilerCheckpointTests(unittest.TestCase):
    def test_changed_compiler_requires_saved_accepted_frontier_before_installation(self):
        import json
        import os
        import sqlite3

        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            (state / "bge").mkdir()
            with sqlite3.connect(state / "bge/queue.sqlite") as db:
                db.execute("CREATE TABLE rkb_control_spool_meta(id INTEGER, accepted_sequence INTEGER)")
                db.execute("INSERT INTO rkb_control_spool_meta VALUES(1,1040)")
                db.execute("CREATE TABLE rkb_dataset_recovery_store(checkpoint_json TEXT, latest_json TEXT)")
                db.execute("INSERT INTO rkb_dataset_recovery_store VALUES(?,?)",
                           (json.dumps({"base_sequence": 948}), json.dumps({"base_sequence": 1019})))
            source = state / "candidate"
            # No jobs/intake rows are needed: accepted-but-unsaved commands
            # remain a replay obligation after their native APPLIED receipt.
            with mock.patch.object(deploy, "STATE", state):
                for running, candidate, saved, refused in (
                    ("old", "new", 1019, True),
                    ("same", "same", 1019, False),
                    ("old", "new", 1040, False),
                ):
                    with self.subTest(running=running, candidate=candidate, saved=saved):
                        with sqlite3.connect(state / "bge/queue.sqlite") as db:
                            db.execute("UPDATE rkb_dataset_recovery_store SET latest_json=?",
                                       (json.dumps({"base_sequence": saved}),))
                        with mock.patch.object(deploy, "cmd", side_effect=[str(os.getpid()), running, candidate]) as commands:
                            if refused:
                                with self.assertRaisesRegex(RuntimeError, "checkpoint"):
                                    deploy.compiler_checkpoint_preflight(source)
                            else:
                                deploy.compiler_checkpoint_preflight(source)
                            self.assertIn("PYTHONPATH=" + str(Path.cwd() / "src"), commands.call_args_list[1].args)
                            self.assertIn("PYTHONPATH=" + str(source / "src"), commands.call_args_list[2].args)
                            self.assertIn("materializer_fingerprint", commands.call_args_list[2].args[-1])

    def test_installation_runs_compiler_guard_before_configuration_or_service_effects(self):
        with mock.patch.object(deploy, "materialize", return_value=Path("/candidate")), \
             mock.patch.object(deploy, "verify_imports"), \
             mock.patch.object(deploy, "compiler_checkpoint_preflight", side_effect=RuntimeError("checkpoint required")), \
             mock.patch.object(deploy, "configuration_plan") as configuration, \
             mock.patch.object(deploy, "offline_backup") as backup, \
             mock.patch.object(deploy, "systemctl") as service:
            with self.assertRaisesRegex(RuntimeError, "checkpoint"):
                deploy.run("c" * 40, True)
            configuration.assert_not_called()
            backup.assert_not_called()
            service.assert_not_called()


if __name__ == "__main__":
    unittest.main()
