
#' ignore sigpipe
#' 
#' Avoid SIGPIPE error in scripts using Arrow S3
#' @export
ignore_sigpipe <- function() {
  requireNamespace("decor", quietly = TRUE)
  cpp11::cpp_source(code = '
  #include <csignal>
  #include <cpp11.hpp>
  [[cpp11::register]] void ignore_sigpipes() {
    signal(SIGPIPE, SIG_IGN);
  }
  ')
  ignore_sigpipes()
}

# decor is required for cpp_source but not called.
# R CHECK insists packages be used.
dummy_decor <- function() {
  requireNamespace("decor", quietly = TRUE)
  decor::cpp_decorations()
}

globalVariables("ignore_sigpipes")