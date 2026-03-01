<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# Can I use '<' to move files, specifying the destination first?

No. The `<` character is not used with `mv` to mean “destination first,” and a command like this will not do what you want:

```bash
mv ~/workspaces/project/ < file.txt   # ❌ not valid for moving
```

`<` is a **redirection** operator for standard input, not part of the `mv` syntax. It feeds the contents of a file into a program’s input, but `mv` doesn’t read filenames from stdin, so this won’t move `file.txt` anywhere.

For `mv`, the basic rule is still:

```bash
mv [options] SOURCE... DESTINATION
```

You can, however, put the destination argument *textually* first and then list sources, as long as the **last** argument is the destination:

```bash
mv file1.txt file2.txt ~/workspaces/project/
# Here ~/workspaces/project/ is the destination (last argument)
```

If you want to avoid retyping the destination, use:

- A variable:

```bash
DEST=~/workspaces/project/
mv file1.txt "$DEST"
mv file2.txt "$DEST"
```

- Or history expansion / keys as in the previous answer (`Alt+.` or `!$`) to reinsert the last argument.
<span style="display:none">[^1][^2][^3][^4][^5][^6]</span>

<div align="center">⁂</div>

[^1]: https://discussions.apple.com/thread/254704033

[^2]: https://linuxize.com/post/how-to-move-files-in-linux-with-mv-command/

[^3]: https://leopard-adc.pepas.com/documentation/Darwin/Reference/ManPages/man1/mv.1.html

[^4]: https://support.apple.com/guide/terminal/manage-files-apddfb31307-3e90-432f-8aa7-7cbc05db27f7/mac

[^5]: https://www.macworld.com/article/222558/macos-command-line-copying-moving-files-terminal.html

[^6]: https://simpledev.io/lesson/move-file-or-folder-terminal-1/

