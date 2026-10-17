import hashlib
import importlib.util
import subprocess
import tempfile
import unittest
from pathlib import Path

SPEC = importlib.util.spec_from_file_location(
    "check_secrets", Path(__file__).resolve().parents[1] / "check-secrets.py"
)
SCANNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SCANNER)


class SecretCheckTest(unittest.TestCase):
    def test_known_credential_in_sql_and_env_fallback(self) -> None:
        value = b"a1b2c3d4" * 4
        fingerprints = {hashlib.sha256(value).hexdigest()}
        for data in (
            b"INSERT INTO t VALUES ('" + value + b"');",
            b"key: ${KEY:" + value + b"}",
            b"API_KEY=" + value,
        ):
            result = SCANNER.findings("settings.yml", data, fingerprints)
            self.assertTrue(
                any(
                    reason == "known local credential"
                    for _, _, reason in result
                )
            )
            self.assertNotIn(value.decode(), repr(result))

    def test_provider_token_and_escaped_assignment(self) -> None:
        value = b"a1b2C3d4E5f6g7H8i9J0k1L2m3N4"
        for data in (b"sk-" + value, b'\\"apiKey\\":\\"' + value + b'\\"'):
            self.assertTrue(SCANNER.findings("seed.sql", data, set()))

    def test_safe_placeholders_and_environment_references(self) -> None:
        data = (
            b"api-key: ${SPARK_API_KEY:}\napi-secret: REPLACE_WITH_YOUR_KEY\n"
            b"api_key = YOUR_LONG_API_KEY_VARIABLE\n"
        )
        self.assertEqual([], SCANNER.findings("application.yml", data, set()))

    def test_code_expressions_and_dummy_private_key_are_not_credentials(
        self,
    ) -> None:
        data = b"String accessToken = getAccessTokenFromService(service);"
        self.assertEqual([], SCANNER.findings("Service.java", data, set()))
        data = (
            b"-----BEGIN "
            + b"PRIVATE KEY-----\\nxxx\\n-----END PRIVATE KEY-----"
        )
        self.assertEqual([], SCANNER.findings("ServiceTest.java", data, set()))
        data = (
            Path(__file__).resolve().parents[1] / "check-secrets.py"
        ).read_bytes()
        self.assertEqual(
            [], SCANNER.findings("scripts/check-secrets.py", data, set())
        )

    def test_private_files_are_rejected_even_without_recognized_tokens(
        self,
    ) -> None:
        for name in (
            ".local/secrets.properties",
            "core/workflow/config.env",
            ".env.custom",
            "client.pem",
        ):
            self.assertTrue(SCANNER.findings(name, b"empty", set()))
        self.assertEqual(
            [], SCANNER.findings("config.env.example", b"empty", set())
        )

    def test_staged_content_and_old_commits_are_scanned(self) -> None:
        original_root = SCANNER.ROOT
        try:
            with tempfile.TemporaryDirectory() as directory:
                SCANNER.ROOT = Path(directory)

                def git(*args: str) -> bytes:
                    return subprocess.check_output(
                        ["git", *args],
                        cwd=directory,
                        stderr=subprocess.DEVNULL,
                    )

                git("init", "-q")
                git("config", "user.email", "test@example.invalid")
                git("config", "user.name", "Test")
                value = b"sk-" + b"a1B2c3D4" * 4
                path = Path(directory) / "config.yml"
                path.write_bytes(b"api-key: " + value)
                git("add", ".")
                path.write_bytes(b"api-key: ${API_KEY:}")
                self.assertTrue(SCANNER.scan(SCANNER.entries(), set(), set()))
                git(
                    "-c",
                    "core.hooksPath=/dev/null",
                    "commit",
                    "-qm",
                    "initial",
                )
                git("add", ".")
                git(
                    "-c", "core.hooksPath=/dev/null", "commit", "-qm", "remove"
                )
                self.assertEqual(
                    [], SCANNER.scan(SCANNER.entries("HEAD"), set(), set())
                )
                self.assertTrue(
                    SCANNER.scan(SCANNER.entries("HEAD~1"), set(), set())
                )
        finally:
            SCANNER.ROOT = original_root


if __name__ == "__main__":
    unittest.main()
