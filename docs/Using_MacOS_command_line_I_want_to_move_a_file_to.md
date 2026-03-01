<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# Using MacOS command line I want to move a file to a folder using a command like:

mv file.txt ~/workspaces/project/
How can I do this without having to retype the destination path every time?  Is there a way I can type the destination path first and after that type the file to move?  Then I repeat the command easily, just with a new filename to be moved at the end.

Yes. You can keep reusing the destination path and just change the filename at the end, a few different ways.

## Easiest: Use shell history for the last argument

If you run:

```bash
mv file.txt ~/workspaces/project/
```

next time you can type:

```bash
mv another-file.txt <Alt+.>
```

On macOS Terminal with zsh or bash, pressing **Alt+.** (or Esc then .) inserts the **last argument** of the previous command, which is your destination path.[^1]

So the sequence is:

```bash
mv file1.txt ~/workspaces/project/
# Next command:
mv file2.txt  ⟵ type this, then press Alt+.
# becomes:
mv file2.txt ~/workspaces/project/
```

You can repeat that as many times as you like, just changing the filename.

Alternative: use history expansion text instead of the key, for example:

```bash
mv file2.txt !$
```

`!$` is expanded by the shell to “last argument from previous command,” i.e., `~/workspaces/project/`.[^2][^1]

## Destination first, file second

If you really want to type the destination first, you can do:

```bash
mv ~/workspaces/project/ file.txt
```

and then repeat with:

```bash
mv ~/workspaces/project/ another-file.txt
```

This is valid: `mv` just needs “source(s) then destination,” and the destination can be the first word as long as all but the last are sources.[^3][^4]

To avoid retyping the destination at all, store it in a variable:

```bash
DEST=~/workspaces/project/
mv file1.txt "$DEST"
mv file2.txt "$DEST"
```

Now you only change the filename each time.
<span style="display:none">[^10][^11][^12][^13][^14][^15][^5][^6][^7][^8][^9]</span>

<div align="center">⁂</div>

[^1]: https://ostechnix.com/insert-last-command-argument-bash-fish-zsh/

[^2]: https://zsh.sourceforge.io/Doc/Release/Expansion.html

[^3]: https://support.apple.com/guide/terminal/manage-files-apddfb31307-3e90-432f-8aa7-7cbc05db27f7/mac

[^4]: https://simpledev.io/lesson/move-file-or-folder-terminal-1/

[^5]: https://discussions.apple.com/thread/254704033

[^6]: https://stackoverflow.com/questions/72017601/mac-terminal-move-multiple-files-located-in-csv-to-different-directory

[^7]: https://www.macworld.com/article/222558/macos-command-line-copying-moving-files-terminal.html

[^8]: https://rpm.pbone.net/changelog_idpl_135955648_com_coreutils-debuginfo-9.5-6.1.i586.rpm.html

[^9]: https://www.reddit.com/r/MacOS/comments/1j48gj6/understanding_folder_replacement_in_macos/

[^10]: https://www.scribd.com/document/619490199/Forensics-and-Incident-Response-Question-BANK-Fair-for-Students-Mcqq

[^11]: https://thevaluable.dev/zsh-expansion-guide-example/

[^12]: https://www.edrawmind.com/mind-maps/44844/network-engineer-linux-server-configuration-and-operation-commands/?lang=EN

[^13]: https://stackoverflow.com/questions/14657391/zsh-expand-a-previous-argument-in-the-current-command-line

[^14]: https://lwn.net/Articles/829737/

[^15]: https://www.reddit.com/r/zsh/comments/vdtsih/how_to_clear_the_history_of_zsh_shell_in_macos/

