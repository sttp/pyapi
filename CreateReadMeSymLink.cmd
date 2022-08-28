:: Need symbolic link to README.md in docs folder so that twine can access markdown file
:: because files outside of current directory are sandboxed in twine
mklink README.md docs\README.md