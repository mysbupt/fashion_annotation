# fashion_annotation
This is a platform for browsing, annotating, query on the large scale fashion images.
/explore for browsing and annotating all the images (require login), and associate metadata, annotations.
/query for query the fashion knowledge (triplets of "occasion, person, clothes") and associate images, we can also show the trending curve of each triplet.

## Requirements
uwsgi
python2.7
flask


## Brief Introduction of Implementation
We use Vue.js for the frontend and Flask for the backend. We use MySQL to store all the relational data like metadata of images, attributes of clothes, and all the annotations. We use Cassandra to store all the images. We put the Flask app behind uwsgi to handle multiple users' access. 
