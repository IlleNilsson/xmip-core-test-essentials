//! What the Playground's far ends ask of a login: the password a node
//! presents proven by `authenticate/password`, the gate a Receive
//! Location's own is, handed to Xmip's SSH and FTP servers as their check
//! (`net::login::Check`) — they link no authenticator (open problem 59).
//! Each password is minted here, with full entropy, and the far end keeps
//! only its salted verifier.

use std::sync::Arc;

use authenticate::store::CredentialStore;
use authenticate_password::PasswordAuthenticator;
use net::login::{Check, Offered};

/// The iterations a minted password's verifier is derived with: one. A
/// secret of full entropy needs no stretching against a guess, and a login
/// stays well under a millisecond.
const MINTED_ITERATIONS: u32 = 1;

/// A password of full entropy, as hexadecimal: 32 random bytes.
#[must_use]
pub fn minted() -> String {
    codec::hex::encode(&codec::random::array::<32>())
}

/// A check admitting each `(user, password)` of `enrolled` alone, through
/// the password gate.
pub fn checking<'a>(enrolled: impl IntoIterator<Item = (&'a str, &'a str)>) -> Check {
    let gate =
        PasswordAuthenticator::new(CredentialStore::from_entries(MINTED_ITERATIONS, enrolled));
    Arc::new(move |offered: &Offered<'_>| gate.admits(offered.user, offered.secret))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_far_end_admits_the_minted_password_of_its_user_alone() {
        let (first, second) = (minted(), minted());
        assert_ne!(first, second, "drawn each time");
        assert_eq!(first.len(), 64);
        let check = checking([("from-a", first.as_str()), ("to-a", second.as_str())]);
        let offered = |user, secret| Offered { user, secret };
        assert!(check(&offered("from-a", &first)));
        assert!(check(&offered("to-a", &second)));
        assert!(!check(&offered("from-a", &second)), "another user's");
        assert!(!check(&offered("from-b", &first)), "a user not enrolled");
    }
}
