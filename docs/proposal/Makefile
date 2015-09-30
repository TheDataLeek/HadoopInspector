TEXDIR = ./tex/

all:
	pdflatex --shell-escape -interaction=batchmode $(TEXDIR)HadoopInspector.tex
	pdflatex --shell-escape -interaction=batchmode $(TEXDIR)HadoopInspector.tex
	pdflatex --shell-escape -interaction=batchmode $(TEXDIR)HadoopInspector.tex

clean:
	for FILE in `find . -type f | grep -e "\.pyg\|\.aux\|\.log\|\.toc\|\.out\|\.equ\|\.lof\|\.tar\.gz"`; do rm $$FILE; done

push:
	echo "Not Implemented"
