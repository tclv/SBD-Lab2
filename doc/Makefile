INPUT := $(wildcard *.text)


PANDOC=pandoc

PANDOC_OPTIONS=--filter pandoc-citeproc -S --normalize --standalone --highlight-style=pygments --toc -f markdown
PANDOC_OPTIONS_LATEX=--latex-engine=xelatex --variable papersize=a4 --variable documentclass=memoir \
		     --variable classoption=article --variable mainfont="TeX Gyre Pagella" \
		     --variable mathfont="TeX Gyre Pagella" --variable monofont="Source Code Pro" \
		     -H preamble.tex

PANDOC_OPTIONS_MD= -t markdown_github --wrap=preserve 

EXPORTED_DOCS=\
 $(INPUT:.text=.pdf)\
 $(INPUT:.text=.md)\

%.pdf : %.text
	$(PANDOC) $(PANDOC_OPTIONS) $(PANDOC_OPTIONS_LATEX) -o $@ $<

%.md : %.text
	$(PANDOC) $(PANDOC_OPTIONS) $(PANDOC_OPTIONS_MD) -o $@ $<

.PHONY: all clean

all : $(EXPORTED_DOCS)

clean: 
	rm $(EXPORTED_DOCS)
