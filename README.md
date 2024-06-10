### Building knowledge vectors from Wiki dumps

The following code can be used to build knowledge vectors from wikidumps. 

In order to do this you will need to download a dump. This is an easy enough thing to source online. I used [this site (May, 2024)](https://wikimedia.bringyour.com/enwiki/20240501/), but there are many mirrors to choose from. A simple Google search should get you where you're going. From here there a bunch of choices of things to download. In the context of this I used the main pages as a single dump `enwiktionary-latest-pages-articles.xml`. However, some downloads represent slices of the whole, and others that represent aspects of pages--and this would certainly be worth considering in the future for a more surgical approach.  The main dump can also be easily parsed using `import lxml.etree as etree`. The advantage is that it doesn't have to be loaded all into memory at once. The disadvantage is that it is not as easily multiprocessed, and it isn't indexed. 

### Steps

* #### Identify page titles
    * This is the easiest and can be done a few ways. The simplest is to download `enwiki-20240501-all-titles`, which is a txt file of all the pages. Alternatively a single pass through the `enwiktionary-latest-pages-articles.xml` where you grab the title of every page will give you the same thing. In either case you might want to do some filtering. ***!CAREFUL!*** certain forms of preprocessing (such as .lower()) could have unintended consequences. Within the dump some pages are essentially redirect pages, so 'bill clinton' might in fact be a page, but it is not the same page as 'Bill Clinton', one is generally a redirect to another, and thus contains a link to the other, but nothing else.
    *  One form of filtering is to remove pages that have little or no links. To do this you can use `filter_wikipedia.py`, running this script will produce a new filtered dump which maintain only pages. 
* #### Identify verbs of interest
    * I had previously scraped wiktionary and built a verb framework. Like all the other things, I could go back and do better, but it gave me a fairly large pool of verbs to use. This is in the data folder as `verb_tree`. For our purposes we are going to want to turn this into a straight list. Wer'e going to let Wikipedia do the heaving lifting in a couple of ways: Identify the commonality of words, which is useful
* Map pages titles to page links
* Optionally map page titles to verbs
* Identify pages with a high degree of link similarity
    * At this stage we have a lot of choices. I decided to 