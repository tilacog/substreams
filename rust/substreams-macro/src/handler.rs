
use proc_macro::TokenStream;
use std::borrow::Borrow;
use proc_macro2::{Span, TokenTree};
use quote::{quote, ToTokens, format_ident};
use syn::{spanned::Spanned, Type};
use crate::errors;
use crate::config::{ModuleType, FinalConfiguration};

pub fn main(_args: TokenStream, item: TokenStream, module_type: ModuleType) -> TokenStream {
    let original = item.clone();

    // let config_result = AttributeArgs::parse_terminated.parse(args)
    //     .and_then(|args| build_config(args));
    //
    // let final_config = match config_result {
    //     Ok(f) => f,
    //     Err(e) => {
    //         return token_stream_with_error(original, e)
    //     }
    // };

    let final_config = FinalConfiguration { module_type };
    let input = syn::parse_macro_input!(item as syn::ItemFn);

    let output_result = parse_func_output(&final_config, input.sig.output.clone());
    match output_result {
        Ok(_) => {}
        Err(e) => {
            return token_stream_with_error(original, e)
        }
    }
    let mut has_seen_writable_store = false;
    let mut args : Vec<proc_macro2::TokenStream> = Vec::with_capacity(input.sig.inputs.len() * 2);
    let mut proto_decodings: Vec<proc_macro2::TokenStream> = Vec::with_capacity(input.sig.inputs.len());
    let mut read_only_stores: Vec<proc_macro2::TokenStream> = Vec::with_capacity(input.sig.inputs.len());
    let mut writable_store: proc_macro2::TokenStream = quote! {};

    //PatType { attrs: [], pat: Ident(PatIdent { attrs: [], by_ref: None, mutability: None, ident: Ident { ident: "transfers", span: #0 bytes(31981..31990) }, subpat: None }), colon_token: Colon, ty: Path(TypePath { qself: None, path: Path { leading_colon: None, segments: [PathSegment { ident: Ident { ident: "erc721", span: #0 bytes(31992..31998) }, arguments: None }, Colon2, PathSegment { ident: Ident { ident: "Transfers", span: #0 bytes(32000..32009) }, arguments: None }] } }) }
    //PatType { attrs: [], pat: Ident(PatIdent { attrs: [], by_ref: None, mutability: None, ident: Ident { ident: "pairs", span: #0 bytes(32015..32020) }, subpat: None }), colon_token: Colon, ty: Reference(TypeReference { and_token: And, lifetime: None, mutability: None, elem: ImplTrait(TypeImplTrait { impl_token: Impl, bounds: [Trait(TraitBound { paren_token: None, modifier: None, lifetimes: None, path: Path { leading_colon: None, segments: [PathSegment { ident: Ident { ident: "store", span: #0 bytes(32028..32033) }, arguments: None }, Colon2, PathSegment { ident: Ident { ident: "StoreGet", span: #0 bytes(32035..32043) }, arguments: None }] } })] }) }) }

    for i in (&input.sig.inputs).into_iter() {
        match i {
            syn::FnArg::Receiver(_) => {
                return token_stream_with_error(original, syn::Error::new(i.span(), format!("handler function does not support 'self' receiver")));
            },
            syn::FnArg::Typed(pat_type) => {
                match &*pat_type.pat {
                    syn::Pat::Ident(v) => {
                        let var_name = v.ident.clone();

                        let argument_type = &*pat_type.ty;
                        let input_res = parse_input_type(argument_type);
                        if input_res.is_err() {
                            return token_stream_with_error(original, syn::Error::new(pat_type.span(), format!("foo {:?}",input_res.err())));
                        }
                        let input_obj = input_res.unwrap();

                        if input_obj.is_writable_store {
                            if has_seen_writable_store {
                                return token_stream_with_error(original, syn::Error::new(pat_type.span(), format!("handler cannot have more then one writable store as an input")));
                            }
                            has_seen_writable_store = true;
                            let trait_type = format_ident!("{}", input_obj.resolved_ty);
                            let extern_type = format_ident!("Extern{}", input_obj.resolved_ty);
                            writable_store = quote! { let #var_name: &dyn store::#trait_type = &store::#extern_type::new(); };
                            continue
                        }

                        if input_obj.is_readable_store {
                            let var_idx = format_ident!("{}_idx",var_name);
                            args.push(quote! { #var_idx: u32 });

                            let trait_type = format_ident!("{}", input_obj.resolved_ty);
                            let extern_type = format_ident!("Extern{}", input_obj.resolved_ty);
                            read_only_stores.push(quote! { let #var_name: &dyn store::#trait_type = &store::#extern_type::new(#var_idx); });
                            continue
                        }


                        if final_config.module_type == ModuleType::Store && var_name.to_string().ends_with("_idx") {
                            args.push(quote! { #pat_type });
                            continue
                        }
                        let var_ptr = format_ident!("{}_ptr", var_name);
                        let var_len = format_ident!("{}_len", var_name);
                        args.push(quote! { #var_ptr: *mut u8 });
                        args.push(quote! { #var_len: usize });

                        if input_obj.is_deltas {
                            proto_decodings.push(quote! { let #var_name: #argument_type = substreams::proto::decode_ptr::<substreams::pb::substreams::StoreDeltas>(#var_ptr, #var_len).unwrap().deltas; })
                        } else {
                            proto_decodings.push(quote! { let #var_name: #argument_type = substreams::proto::decode_ptr(#var_ptr, #var_len).unwrap(); })
                        }
                    },
                    _ => {
                        return token_stream_with_error(original, syn::Error::new(pat_type.span(), format!("unknown argument type")));
                    }
                }
            },
        }
    }


    match final_config.module_type {
        ModuleType::Store => build_store_handler(input, args, proto_decodings, read_only_stores, writable_store),
        ModuleType::Map => build_map_handler(input, args, proto_decodings, read_only_stores, writable_store)
    }
}

const WRITABLE_STORE: [&'static str; 15] = [
    "StoreSet",
    "StoreSetIfNotExists",
    "StoreAddInt64",
    "StoreAddFloat64",
    "StoreAddBigFloat",
    "StoreAddBigInt",
    "StoreMaxInt64",
    "StoreMaxBigInt",
    "StoreMaxFloat64",
    "StoreMaxBigFloat",
    "StoreMinInt64",
    "StoreMinBigInt",
    "StoreMinFloat64",
    "StoreMinBigFloat",
    "StoreAppend"
];
const READABLE_STORE: [&'static str; 1] = ["StoreGet"];

#[derive(Debug)]
struct Input {
    is_writable_store: bool,
    is_readable_store: bool,
    is_deltas: bool,
    resolved_ty: String
}

// fn print_type_of<T>(_: &T) -> String{
//     format!("{}", std::any::type_name::<T>())
// }
fn parse_input_type(ty: &syn::Type) -> Result<Input, errors::SubstreamMacroError> {
    match ty {
        syn::Type::Reference(r) => {
            // println!("Reference: {:?}", r);

            // TypePath { qself: None, path: Path { leading_colon: None, segments: [PathSegment { ident: Ident { ident: "erc721", span: #0 bytes(31992..31998) }, arguments: None }, Colon2, PathSegment { ident: Ident { ident: "Transfers", span: #0 bytes(32000..32009) }, arguments: None }] } }
            // TypeReference { and_token: And, lifetime: None, mutability: None, elem: ImplTrait(TypeImplTrait { impl_token: Impl, bounds: [Trait(TraitBound { paren_token: None, modifier: None, lifetimes: None, path: Path { leading_colon: None, segments: [PathSegment { ident: Ident { ident: "store", span: #0 bytes(32028..32033) }, arguments: None }, Colon2, PathSegment { ident: Ident { ident: "StoreGet", span: #0 bytes(32035..32043) }, arguments: None }] } })] }) }
            let mut input = Input{
                is_writable_store: false,
                is_readable_store: false,
                is_deltas: false,
                resolved_ty: "".to_owned()
            };
            // println!("Elem: {}", print_type_of(&r.elem));
            let mut param_name = "".to_owned();
            let mut param_type = "".to_owned();
            match r.elem.borrow() {
                Type::ImplTrait(i) => {
                    let bound = i.bounds.last().unwrap();
                    for b in bound.to_token_stream() {
                        match b {
                            TokenTree::Ident(ident) => {
                                if param_name == "" {
                                    param_name = ident.to_string();
                                    continue
                                }
                                if param_type == "" {
                                    param_type = ident.to_string()
                                }
                            }
                            _ =>    {
                                continue
                            }
                        };
                    }
                    // println!("Param {} of type {}", param_name, param_type);

                }
                _ => {
                    return Err(errors::SubstreamMacroError::UnknownInputType("Expected trait implementation".to_owned()))
                }
            }
            for t in WRITABLE_STORE {
                if param_type == t.to_owned() {
                    input.is_writable_store = true;
                    input.resolved_ty = param_type;
                    return Ok(input);
                }
            }
            for t in READABLE_STORE {
                if param_type == t.to_owned() {
                    input.is_readable_store = true;
                    input.resolved_ty = param_type;
                    return Ok(input);
                }
            }
            Ok(input)
        },
        syn::Type::Path(p) => {
            // println!("Path: {:?}", p);
            let mut input = Input{
                is_writable_store: false,
                is_readable_store: false,
                is_deltas: false,
                resolved_ty: "".to_owned()
            };
            let mut last_type = "".to_owned();
            for segment in p.path.segments.iter() {
                    last_type = segment.ident.to_string();
            }
            if last_type == "Deltas".to_owned() {
                // todo: should check that it's fully qualified to be our `store::Deltas`
                input.is_deltas = true;
            }
            Ok(input)
        }
        _ => {
            // fixme(@julien): return the type which is considered an error
            Err(errors::SubstreamMacroError::UnknownInputType("asdfasd".to_owned()))
        }
    }
}


fn parse_func_output(final_config: &FinalConfiguration, output: syn::ReturnType) -> Result<(), syn::Error> {
    match final_config.module_type {
        ModuleType::Map => {
            if output == syn::ReturnType::Default {
                return Err(syn::Error::new(Span::call_site(), "Module of type Map should have a return of type Result<YOUR_TYPE, SubstreamError>"));
            }

            let expected = vec!["-".to_owned(), ">".to_owned(), "Result".to_owned()];
            let mut index = 0;
            let mut valid = true;
            for i in output.into_token_stream().into_iter() {
                if index == expected.len() {
                    if valid {
                        return Ok(())
                    } else {
                        return Err(syn::Error::new(Span::call_site(), "Module of type Map should return a Result<>"));
                    }
                }
                if i.to_string() != expected[index] {
                    valid = false
                }
                index += 1;
            }
            return Ok(())
        },
        ModuleType::Store => {
            if output != syn::ReturnType::Default {
                return Err(syn::Error::new(Span::call_site(), "Module of type Store should not have a return statement"));
            }
            return Ok(())
        }
    }
}

fn build_map_handler(input: syn::ItemFn, collected_args: Vec<proc_macro2::TokenStream>, decodings: Vec<proc_macro2::TokenStream>, read_only_stores: Vec<proc_macro2::TokenStream>, writable_store: proc_macro2::TokenStream) -> TokenStream {
    let body = &input.block;
    let header = quote! {
        #[no_mangle]
    };
    let func_name = input.sig.ident.clone();
    let lambda_return = input.sig.output.clone();
    let lambda = quote! {
        let func = || #lambda_return {
            #(#decodings)*
            #(#read_only_stores)*
            #writable_store
            #body
        };
    };
    let result = quote! {
        #header
        pub extern "C" fn #func_name(#(#collected_args),*){
            substreams::register_panic_hook();
            #lambda
            let result = func();
            if result.is_err() {
                panic!(result.err().unwrap())
            }
            substreams::output(result.unwrap());
        }
    };
    result.into()
}

fn build_store_handler(input: syn::ItemFn, collected_args: Vec<proc_macro2::TokenStream>, decodings: Vec<proc_macro2::TokenStream>, read_only_stores: Vec<proc_macro2::TokenStream>, writable_store: proc_macro2::TokenStream) -> TokenStream {
    let body = &input.block;
    let header = quote! {
        #[no_mangle]
    };
    let func_name = input.sig.ident.clone();
    let result = quote! {
        #header
        pub extern "C" fn #func_name(#(#collected_args),*){
            substreams::register_panic_hook();
            #(#decodings)*
            #(#read_only_stores)*
            #writable_store
            #body
        }
    };
    result.into()
}


fn token_stream_with_error(mut tokens: TokenStream, error: syn::Error) -> TokenStream {
    tokens.extend(TokenStream::from(error.into_compile_error()));
    tokens
}
