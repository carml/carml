# NOTES

metadata.nt contains some invalid triples. Replace them as follows:

regex:
```
^(.*) <http://www.w3.org/2006/03/test-description#specificationReference> <>.$
```

replacement:
```
#$1 <http://www.w3.org/2006/03/test-description#specificationReference> <>.
```
