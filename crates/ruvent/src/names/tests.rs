use super::*;

#[test]
fn equal_texts_share_one_handle() {
    let mut names = Names::default();
    let a = names.intern("orders");
    let b = names.adopt(&Name::new("orders"));
    assert!(Arc::ptr_eq(&a.0, &b.0));
    assert_eq!(names.len(), 1);
}

#[test]
fn one_off_names_do_not_grow_the_dictionary_without_bound() {
    let mut names = Names::default();
    let kept = names.intern("kept");
    for index in 0..100_000 {
        // Each name is dropped as soon as it is interned, like a one-off reply channel.
        let _ = names.intern(&format!("reply.{index}"));
        assert!(names.len() <= 2 * 2 + FLOOR, "dictionary grew to {} at {index}", names.len());
    }
    assert_eq!(&*names.intern("kept"), "kept");
    assert!(Arc::ptr_eq(&names.intern("kept").0, &kept.0), "a live name is never swept");
}

#[test]
fn a_name_held_elsewhere_survives_a_sweep() {
    let mut names = Names::default();
    let held: Vec<Name> = (0..FLOOR).map(|index| names.intern(&format!("held.{index}"))).collect();
    let _ = names.intern("trigger");
    for name in &held {
        assert!(Arc::ptr_eq(&names.intern(name).0, &name.0));
    }
}
