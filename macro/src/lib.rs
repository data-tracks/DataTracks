use proc_macro::TokenStream;
use quote::quote;
use std::time::Duration;
use syn::punctuated::Punctuated;
use syn::Expr::Lit;
use syn::Lit::Int;
use syn::{parse_macro_input, Attribute, Ident, ItemFn, Meta, Token};

/// A procedural macro to time a test and fail it if it exceeds a duration.
///
/// Usage:
/// ```
/// use r#macro::limited;
///
/// #[limited(s = 100)]
/// fn my_test() {
///     // ... test code
/// }
///
/// #[limited(ms = 1000)]
///  fn my_other_test() {
///     // ... test code
/// }
/// ```
#[proc_macro_attribute]
pub fn limited(args: TokenStream, item: TokenStream) -> TokenStream {
    // Parse the macro arguments (e.g., duration_ms = 1000)
    let mut duration = Duration::from_millis(1000);
    let args: Punctuated<Meta, Token![,]> =
        parse_macro_input!(args with Punctuated::parse_terminated);
    if let Some(Meta::NameValue(nv)) = args.first()
        && let Some(ident) = nv.path.get_ident()
    {
        let abri = ident.to_string();

        if let Lit(expr_lit) = &nv.value
            && let Int(lit_int) = &expr_lit.lit
        {
            let num = lit_int.base10_parse().unwrap_or(1000);
            if abri == "ms" {
                duration = Duration::from_millis(num);
            } else if abri == "s" {
                duration = Duration::from_secs(num)
            } else if abri == "min" {
                duration = Duration::from_secs(num * 60)
            }
        }
    };

    // Parse the original function
    let mut input = parse_macro_input!(item as ItemFn);
    let original_fn_name = &input.sig.ident.clone();

    let new_fn_name = Ident::new(
        &format!("_timed_{}", original_fn_name),
        original_fn_name.span(),
    );

    let helper_fn_name = Ident::new(
        &format!("_timed_helper_{}", original_fn_name),
        original_fn_name.span(),
    );

    // Rename the original function to avoid conflicts
    input.sig.ident = new_fn_name.clone();

    // Extract and remove the attributes we care about
    // You would typically loop through and check the attribute's path.
    let attributes: Vec<Attribute> = input.attrs.drain(..).collect();

    let duration_ms = duration.as_millis() as u64; // for this case this should be ok

    let generated = quote! {

        // This is the generated code that replaces the original function
        //#[test]
        fn #original_fn_name() {
            let start = std::time::Instant::now();
            let duration_limit = std::time::Duration::from_millis(#duration_ms);

            let finished = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let finished_clone = finished.clone();

            // Execute the helper in parallel
            let handle = std::thread::Builder::new()
                .name("test_runner".to_string())
                .spawn(||#helper_fn_name(finished_clone)).unwrap();


            while !finished.load(std::sync::atomic::Ordering::SeqCst) && !handle.is_finished() {
                if start.elapsed() > duration_limit {
                    panic!("Test duration of {:?} exceeded the limit of {:?}", start.elapsed(), duration_limit);
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            handle.join().unwrap();

        }

        fn #helper_fn_name(flag: std::sync::Arc<std::sync::atomic::AtomicBool>) {
            // Execute the original test function
            #new_fn_name();
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        }

        // The original function, renamed to be called by the wrapper
        #input
    };
    // re-apply the attributes
    let extend = quote! {
        #(#attributes)*
        #generated
    };

    extend.into()
}
