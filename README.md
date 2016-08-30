# Distortion

### Auto-parallelizing, software transactional memory runtime

----



----

#### Documentation is at the wiki



### How to use

Create a runtime

    Distortion distortion = new Distortion(blah, blah, blah);
  
Send shit to it

    distortion.submit(foobar);
    
----

### How to build

Use Ant to run build.xml or have Maven do it for you.

Alternatively, you can just run javac on everything and jar the resulting goo.

----

### How to submit bug reports / feature requests

Currently, I'm just using Bitbucket's integrated tracker.
Do try to include JVM details (version / max heap / gc) if it's more than just a logic error.

Test cases are required for everything. If it's not strictly code related, construct a hypothetical.

----

### How to improve Distortion

First, you'll have to sign the CLA. Distortion has a proprietary branch and your work will probably end up in it.

