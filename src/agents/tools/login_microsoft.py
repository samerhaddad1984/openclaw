from pathlib import Path
import msal

CLIENT_ID = "11da5dd7-6b6f-4367-9815-562805ae9b40"
TENANT_ID = "040521f0-f6fe-42fb-b749-286867861874"

SCOPES = [
    "User.Read",
    "Mail.ReadWrite",
    "Mail.Send",
    "Sites.ReadWrite.All",
]

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
TOKEN_FILE = DATA_DIR / "tokens.json"

DATA_DIR.mkdir(parents=True, exist_ok=True)

cache = msal.SerializableTokenCache()
if TOKEN_FILE.exists():
    cache.deserialize(TOKEN_FILE.read_text(encoding="utf-8"))

app = msal.PublicClientApplication(
    CLIENT_ID,
    authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    token_cache=cache,
)

flow = app.initiate_device_flow(scopes=SCOPES)
if "user_code" not in flow:
    print("❌ Device code flow failed:\n", flow)
    raise SystemExit(1)

print("\nLOGIN REQUIRED (manual)")
print(flow["message"])  # includes URL + code
print("\nWaiting...\n")

result = app.acquire_token_by_device_flow(flow)

if "access_token" in result:
    TOKEN_FILE.write_text(cache.serialize(), encoding="utf-8")
    print("\n✅ Login successful. Token saved to:", TOKEN_FILE)
else:
    print("\n❌ Login failed:\n", result)