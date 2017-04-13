# TwittMap
NYU Tondon CS-GY9223 Cloud Computing Course Project

### Group member: Zeyu Meng(zm649) Bingxin Chen(bc1958) 

![alt text](https://raw.githubusercontent.com/ChronoResister/TwittMap/master/screenshot.jpeg "Screenshot")

##Using Java Servlet and Tomcat 

- Streaming.java: Run on aws beanstalk worker environment. Uploading streaming to aws elasticsearch
- DElete.java: Run on aws beanstalk worker environment. Delete previous tweets uploaded a week ago.
- TwittMap.java: servlet backend which searches the tweets and sends them to frontend for displaying.  
- frontend: use markers with different colors to represent different keywords. Staying mouse on markers will show the twitter text. Initial webpage shows all the keywords.


##How to use: 
- Using Streaming.java to add some data in your aws elasticsearch or offline. 
- Create aws project with aws sdk in eclipse. Integrate it as a serlet project with Tomcat, and build the project clearly
- test the frontend in localhost
- deploy the streaming and delete function on aws beanstalk worker environment, then backend and frontend on aws beanstalk web app. 

