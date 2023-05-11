
#[macro_export]
macro_rules! debug {
    ($($val:expr),+ $(,)?) => {{
        use colored::Colorize;

        eprintln!("{}", format!("[{}:{}]", file!(), line!()).yellow());
        $(
            match $val {
                tmp => {
                    let tmp_string = format!("{:#?}", &tmp);
                    eprint!("    {} = ", stringify!($val).green());
                    for (i, l) in tmp_string.lines().enumerate() {
                        if (i == 0) {
                            eprintln!("{}", l);
                        } else {
                            eprintln!("    {}", l);
                        }
                    }
                }
            };
        )*
    }}
}