# Figure Review

Book titles and the original dataset/table identifiers are retained as factual source attribution and filter labels. The plots and SQL screenshots do not need blanket anonymization for the chosen publication approach. All 31 image files are unchanged.

## Remaining manual cleanup

The following two original images remain in the repository. They are no longer embedded in the README, which now uses Mermaid diagrams with invented examples. Edit or remove these originals before sharing a repository snapshot intended to exclude corpus examples.

| File | What to change |
| --- | --- |
| [gin-index.png](diagrams/gin-index.png) | Replace the three prose samples in Step 1, whose provenance is unverified, with explicitly invented sentences. Update the derived lexemes and illustrative posting-list entries to match. Preserve the measured timings and counts. |
| [search-patterns.png](diagrams/search-patterns.png) | Replace the corpus-specific question and keyword list with the invented example from the dataset guide: Who guards the castle? Keywords: guards, castle. Book-title filter labels can remain. |

## Completed

- Replaced both image embeds in the README with diagrams using invented examples.
- Removed source-like prose examples from the README text and the corpus-specific example question from the dataset guide.
- Restored original plot filenames, book labels in all four benchmark CSVs, and table/index identifiers in code and documentation.
- Verified that all 848 CSV rows match the committed measurements and book labels, and that all 31 images are unchanged.
- Removed dataset download links and the embedded vector from the SQL diagnostic script. No EPUB or Parquet dataset files were found in the repository.

Earlier Git revisions and external dataset storage are unchanged. This checklist records publication cleanup; it does not establish permission for the underlying corpus processing.
