import subprocess
import sys
import unittest

from helpers import REPO


class RepoHygieneTests(unittest.TestCase):
    def test_validate_repo_script_passes(self):
        proc = subprocess.run([sys.executable, str(REPO / "scripts" / "validate_repo.py")],
                              capture_output=True, text=True, cwd=REPO)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)


if __name__ == "__main__":
    unittest.main()
