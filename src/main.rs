mod compact;
mod errors;
mod helpers;
mod lsm;

fn main() {}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use tempfile::{tempdir, tempfile};
//     #[test]
//     fn test_get_after_put() -> io::Result<()> {
//         // put value in storage, then retrieve
//         let dir = tempdir()?;
//         let mut db = KVEngine::open(dir.path(), SyncConfig::None)?;
//         db.put("hello", b"world")?;
//         db.sync()?; // make sure we put the data in there
//         assert_eq!(db.get("hello")?, b"world");
//         Ok(())
//     }

//     #[test]
//     fn delete_after_put() -> io::Result<()> {
//         let dir = tempdir()?;
//         let mut db = KVEngine::open(dir.path(), SyncConfig::None)?;
//         db.put("hello", b"world")?;
//         db.sync()?;
//         assert_eq!(db.get("hello")?, b"world");
//         db.delete("hello")?;

//         assert!(db.get("hello").is_err());

//         Ok(())
//     }

//     #[test]
//     fn print_keys() -> io::Result<()> {
//         let dir = tempdir()?;
//         let mut db = KVEngine::open(dir.path(), SyncConfig::None)?;
//         let mut vec: Vec<String> = Vec::new();
//         db.put("hello", b"world")?;
//         db.put("otherkey", b"world")?;
//         db.put("thekey", b"world")?;
//         db.put("space", b"world")?;

//         vec = db.list_keys()?;
//         vec.sort();

//         assert_eq!(vec, vec!["hello", "otherkey", "space", "thekey"]);
//         assert_eq!(vec.len(), 4);
//         Ok(())
//     }

//     #[test]
//     fn merge_files() -> io::Result<()> {
//         let dir = tempdir()?;

//         let mut db = KVEngine::open(dir.path(), SyncConfig::None)?;
//         let mut vec: Vec<String> = Vec::new();
//         db.put("hello", b"world")?;
//         db.put("otherkey", b"world")?;
//         db.put("thekey", b"world")?;
//         db.put("space", b"world")?;
//         db.delete("thekey")?;
//         db.delete("otherkey")?;

//         db.merge()?;
//         db.sync()?;
//         vec = db.list_keys()?;
//         vec.sort();

//         assert_eq!(vec, vec!["hello", "space"]);
//         assert_eq!(vec.len(), 2);

//         assert!(db.get("thekey").is_err());
//         assert!(db.get("otherkey").is_err());

//         let hint_files: Vec<_> = fs::read_dir(dir.path())
//             .unwrap()
//             .filter_map(|e| e.ok())
//             .filter(|e| e.path().extension().unwrap_or_default() == "hint")
//             .collect();
//         assert!(!hint_files.is_empty());
//         Ok(())
//     }

//     /*AVL Tree tests, insertion, deletion, rotations */
// }
