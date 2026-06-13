//! Optional single-user authentication.
//!
//! When `DOCKDOE_AUTH_USER` and `DOCKDOE_AUTH_PASSWORD` are both set, the web UI
//! requires a login. There is no user database: one credential pair lives in the
//! environment. A successful login sets a signed, stateless session cookie —
//! `<expiry>.<HMAC-SHA256(secret, expiry)>` — that the middleware verifies on
//! every request, so credentials never travel again after login.
//!
//! The signing secret is 32 random bytes persisted in the store (see
//! [`crate::store::Store::session_secret`]), so sessions survive restarts —
//! Stefan's "log in once and be done" — without any server-side session table.
//! Deriving the secret from the password instead would let a stolen cookie be
//! brute-forced offline back to the password; a random secret avoids that.

use std::fmt::Write as _;
use std::time::Duration;

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// How long a session cookie stays valid.
// No `Duration::from_days` on stable; same trade-off as the history ranges.
#[allow(clippy::duration_suboptimal_units)]
pub const SESSION_TTL: Duration = Duration::from_secs(30 * 24 * 3600);

/// Name of the session cookie.
pub const COOKIE_NAME: &str = "dockdoe_session";

/// 32 fresh random bytes from the OS CSPRNG, for use as the session secret.
#[must_use]
pub fn generate_secret() -> Vec<u8> {
    let mut buf = [0u8; 32];
    getrandom::fill(&mut buf).expect("OS random source unavailable");
    buf.to_vec()
}

/// Authentication state for the web layer; present only when both credentials
/// are configured.
#[derive(Clone)]
pub struct Auth {
    user: String,
    password: String,
    secret: Vec<u8>,
    /// Mark the cookie `Secure` (sent only over HTTPS). Off by default so login
    /// works over plain http on a LAN; turn on behind a TLS proxy.
    secure_cookie: bool,
}

impl Auth {
    #[must_use]
    pub fn new(user: String, password: String, secret: Vec<u8>, secure_cookie: bool) -> Self {
        Self {
            user,
            password,
            secret,
            secure_cookie,
        }
    }

    /// Constant-time check of submitted credentials against the configured pair.
    /// Both sides are reduced to a fixed-length HMAC tag, so the comparison
    /// leaks neither length nor content through an early exit.
    #[must_use]
    pub fn credentials_valid(&self, user: &str, password: &str) -> bool {
        let expected = self.credential_tag(&self.user, &self.password);
        let mut mac = self.hmac();
        feed_credentials(&mut mac, user, password);
        mac.verify_slice(&expected).is_ok()
    }

    /// `Set-Cookie` value for a fresh session valid for [`SESSION_TTL`].
    #[must_use]
    pub fn issue_cookie(&self, now_unix: u64) -> String {
        let expiry = now_unix + SESSION_TTL.as_secs();
        let token = self.sign(expiry);
        self.cookie(&token, SESSION_TTL.as_secs())
    }

    /// `Set-Cookie` value that expires the session immediately (logout).
    #[must_use]
    pub fn clear_cookie(&self) -> String {
        self.cookie("", 0)
    }

    /// Whether `token` is a currently valid, correctly signed session token.
    #[must_use]
    pub fn token_valid(&self, token: &str, now_unix: u64) -> bool {
        let Some((expiry_str, sig)) = token.split_once('.') else {
            return false;
        };
        let Ok(expiry) = expiry_str.parse::<u64>() else {
            return false;
        };
        if expiry <= now_unix {
            return false;
        }
        let Ok(sig_bytes) = decode_hex(sig) else {
            return false;
        };
        let mut mac = self.hmac();
        mac.update(expiry_str.as_bytes());
        mac.verify_slice(&sig_bytes).is_ok()
    }

    fn sign(&self, expiry: u64) -> String {
        let mut mac = self.hmac();
        let expiry_str = expiry.to_string();
        mac.update(expiry_str.as_bytes());
        format!("{expiry_str}.{}", encode_hex(&mac.finalize().into_bytes()))
    }

    fn cookie(&self, token: &str, max_age: u64) -> String {
        let mut c =
            format!("{COOKIE_NAME}={token}; HttpOnly; SameSite=Lax; Path=/; Max-Age={max_age}");
        if self.secure_cookie {
            c.push_str("; Secure");
        }
        c
    }

    fn credential_tag(&self, user: &str, password: &str) -> Vec<u8> {
        let mut mac = self.hmac();
        feed_credentials(&mut mac, user, password);
        mac.finalize().into_bytes().to_vec()
    }

    fn hmac(&self) -> HmacSha256 {
        // HMAC accepts a key of any length, so this never fails.
        HmacSha256::new_from_slice(&self.secret).expect("HMAC accepts any key length")
    }
}

/// Feed `user` and `password` into a MAC with a separator, so that e.g.
/// (`"ab"`, `"c"`) and (`"a"`, `"bc"`) produce different tags.
fn feed_credentials(mac: &mut HmacSha256, user: &str, password: &str) {
    mac.update(user.as_bytes());
    mac.update(&[0]);
    mac.update(password.as_bytes());
}

/// The value of `name` in a `Cookie` header, if present.
#[must_use]
pub fn cookie_value<'a>(header: &'a str, name: &str) -> Option<&'a str> {
    header.split(';').map(str::trim).find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        (k == name).then_some(v)
    })
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn decode_hex(s: &str) -> Result<Vec<u8>, ()> {
    if !s.len().is_multiple_of(2) {
        return Err(());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| ()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn auth() -> Auth {
        Auth::new(
            "admin".into(),
            "s3cret".into(),
            vec![1, 2, 3, 4, 5, 6, 7, 8],
            false,
        )
    }

    #[test]
    fn credentials_check_is_exact() {
        let a = auth();
        assert!(a.credentials_valid("admin", "s3cret"));
        assert!(!a.credentials_valid("admin", "wrong"));
        assert!(!a.credentials_valid("root", "s3cret"));
        assert!(!a.credentials_valid("", ""));
        // The separator stops field-boundary confusion.
        assert!(!a.credentials_valid("admins", "3cret"));
    }

    #[test]
    fn token_roundtrips_and_respects_expiry() {
        let a = auth();
        let now = 1_000_000;
        // issue_cookie embeds expiry = now + TTL; pull the token back out.
        let cookie = a.issue_cookie(now);
        let token = cookie_value(&cookie, COOKIE_NAME).unwrap();
        assert!(a.token_valid(token, now));
        // Still valid just before expiry, invalid at/after it.
        let expiry = now + SESSION_TTL.as_secs();
        assert!(a.token_valid(token, expiry - 1));
        assert!(!a.token_valid(token, expiry));
    }

    #[test]
    fn token_rejects_tampering_and_wrong_secret() {
        let a = auth();
        let now = 1_000_000;
        let token = a.sign(now + 999);
        assert!(a.token_valid(&token, now));
        // Flip the claimed expiry without re-signing → MAC mismatch.
        let (_, sig) = token.split_once('.').unwrap();
        let forged = format!("{}.{sig}", now + 999_999);
        assert!(!a.token_valid(&forged, now));
        // Garbage shapes are rejected, not panicked on.
        assert!(!a.token_valid("nonsense", now));
        assert!(!a.token_valid("123.zz", now));
        // A different secret can't validate our token.
        let other = Auth::new("admin".into(), "s3cret".into(), vec![9, 9, 9], false);
        assert!(!other.token_valid(&token, now));
    }

    #[test]
    fn secure_flag_controls_cookie_attribute() {
        let plain = Auth::new("u".into(), "p".into(), vec![1, 2, 3], false);
        let secure = Auth::new("u".into(), "p".into(), vec![1, 2, 3], true);
        assert!(!plain.issue_cookie(0).contains("; Secure"));
        assert!(secure.issue_cookie(0).contains("; Secure"));
        // Cleared cookie carries Max-Age=0 and keeps the same attributes.
        assert!(secure.clear_cookie().contains("Max-Age=0"));
        assert!(secure.clear_cookie().contains("; Secure"));
    }

    #[test]
    fn cookie_value_picks_the_right_pair() {
        let header = "foo=bar; dockdoe_session=abc.def; baz=qux";
        assert_eq!(cookie_value(header, COOKIE_NAME), Some("abc.def"));
        assert_eq!(cookie_value(header, "missing"), None);
    }
}
