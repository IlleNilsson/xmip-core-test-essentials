//! What the Essentials' far ends ask of a login: the password a node
//! presents proven by `authenticate/password`, and a key whose signature
//! the server verified held to its user by `authenticate/ssh-key` — the
//! gates a Receive Location's own are — handed to Xmip's SSH and FTP
//! servers as their check (`net::login::Check`); they link no
//! authenticator (open problem 59). Each password is minted here, with full
//! entropy, and the far end keeps only its salted verifier; each key is
//! enrolled for its one user.

use std::sync::Arc;

use authenticate::store::CredentialStore;
use authenticate_password::PasswordAuthenticator;
use authenticate_ssh_key::{AuthorizedKey, Verifier};
use net::login::{Check, Offer};

/// The iterations a minted password's verifier is derived with: one. A
/// secret of full entropy needs no stretching against a guess, and a login
/// stays well under a millisecond.
const MINTED_ITERATIONS: u32 = 1;

/// A password of full entropy, as hexadecimal: 32 random bytes.
#[must_use]
pub fn minted() -> String {
    codec::hex::encode(&codec::random::array::<32>())
}

/// A check admitting each `(user, password)` of `passwords` alone, through
/// the password gate, and each of `keys` for the one user it is narrowed
/// to, through the ssh-key gate.
pub fn checking<'a>(
    passwords: impl IntoIterator<Item = (&'a str, &'a str)>,
    keys: Vec<AuthorizedKey>,
) -> Check {
    let password =
        PasswordAuthenticator::new(CredentialStore::from_entries(MINTED_ITERATIONS, passwords));
    let key = Verifier::new(keys);
    Arc::new(move |offer: &Offer<'_>| match offer {
        Offer::Password(offered) => password.admits(offered.user, offered.secret),
        Offer::Key(offered) => key.admits(offered.user, offered.key),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use codec::writer::ByteWriter;
    use net::login::{Offered, OfferedKey};

    #[test]
    fn a_far_end_admits_the_minted_password_of_its_user_alone() {
        let (first, second) = (minted(), minted());
        assert_ne!(first, second, "drawn each time");
        assert_eq!(first.len(), 64);
        let check = checking(
            [("from-a", first.as_str()), ("to-a", second.as_str())],
            Vec::new(),
        );
        let offered = |user, secret| Offer::Password(Offered { user, secret });
        assert!(check(&offered("from-a", &first)));
        assert!(check(&offered("to-a", &second)));
        assert!(!check(&offered("from-a", &second)), "another user's");
        assert!(!check(&offered("from-b", &first)), "a user not enrolled");
    }

    #[test]
    fn a_far_end_admits_an_enrolled_key_for_its_user_alone() {
        let verifying = ed25519_dalek::SigningKey::from_bytes(&[7; 32]).verifying_key();
        // The key's blob as SSH writes it: two strings, each counted.
        let mut blob = Vec::new();
        for field in [&b"ssh-ed25519"[..], verifying.as_bytes()] {
            blob.u32_be(u32::try_from(field.len()).expect("short"));
            blob.extend_from_slice(field);
        }
        let line = format!("ssh-ed25519 {}", codec::base64::encode(&blob));
        let enrolled = AuthorizedKey::parse(&line)
            .expect("a key")
            .for_user("from-a");
        let check = checking([], vec![enrolled]);
        let offered = |user, key| Offer::Key(OfferedKey { user, key });
        assert!(check(&offered("from-a", &blob)));
        assert!(!check(&offered("to-a", &blob)), "another user");
        assert!(
            !checking([], Vec::new())(&offered("from-a", &blob)),
            "none enrolled"
        );
    }
}
