-- Encrypted investor profile store.
-- profile_id  = SHA256 hex of passphrase (lookup key — not the secret itself)
-- profile_salt = 16-byte random salt, hex-encoded (unique per profile, stored in plain)
-- profile_data = Fernet-encrypted JSON blob (decryptable only with correct passphrase)
CREATE TABLE IF NOT EXISTS system_meta.investor_profiles (
    profile_id   String,
    profile_salt String,
    profile_data String,
    updated_at   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY profile_id;
