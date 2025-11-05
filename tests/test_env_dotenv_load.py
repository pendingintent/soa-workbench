import os
from importlib import reload
import soa_builder.web.app as webapp

TEST_KEY = "FAKE_KEY_123"


def test_dotenv_load_order():
    # Write a temporary .env with API key and remove env var from process to force dotenv usage
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    env_path = os.path.abspath(env_path)
    with open(env_path, "w", encoding="utf-8") as f:
        f.write(f"CDISC_API_KEY={TEST_KEY}\n")
    os.environ.pop("CDISC_API_KEY", None)
    reload(webapp)  # re-import after .env modification
    # fetch concepts should include header when calling remote (we cannot hit remote reliably, but ensure key function returns)
    assert webapp._get_cdisc_api_key() == TEST_KEY
    # cleanup (optional)
    os.remove(env_path)
