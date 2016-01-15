HAMR
============
   Hadoop Annotated MapReduce (HAMR) is designed to simplify the development of shuffle phrase in Hadoop MapReduce and let developers caring less about the details of the Hadoop implementation such as sort comparator, partitioner, and group comparator. 

#usage 

Let me explain the usage of HAMR with examples. All example code and data can be found [here][exampleCode].

##1.secondary sorting with HAMR

  Consider a situation like this: you want to make statistics on how long users stay in web pages. To do so, a script which will send a message to server in every 5 Sec is injected into each page. The format of message is like this: 

messageTime###sessionId###url

messageTime is the time that the script sends that message; sessionid denote a single user; and the url is the page's url. 

  In usual cases, the MapReduce program should be designed that key-value pairs is sorted first by url, then sessionId, and refresh time. On Reduce side, key-value pairs is grouped by url and sessionId so that each reducer gets one visitor's track on one page which is sorted in time.Reducer recives groups like this:

url1 , sessionId1,10:00

url1 , sessionId1,10:05

....

url1 , sessionId1,10:xx

____________________(group 1)

url1 , sessionId2,10:05

url1 , sessionId2,10:10

....

url1 , sessionId2,10:xx

____________________(group 2)

url2 , sessionId1,10:05

url2 , sessionId1,10:10

....

url2 , sessionId1,10:xx

____________________(group 3)

.....

____________________(group n)

  Reducer compares the time in first data it recived with the last one to decide how long the user stay on that page.

  To finish all these works, you need to write mapper, reducer, sortComparator.....

   HAMR makes it much more simple. First put the fields which used to exist in key and value class into a class which extended AnnotedBean class.
```java
    @Generator(keyGeneratorClass = VisitKeyGenerator.class)
    @Counters(counters = { VisitTimeCounter.class })
    public class VisitBean extends AnnotedBean {
	 @SortField(level = 3)
	 @GroupField
	 private String url;
	
	 @SortField(level = 2)
	 @GroupField
	 private String sessionId;

	 @SortField(level = 1)
	 private Long messageTime;

	 @SkipIO
	 private Long totalTime;
    }
```
 

  Let me first explain annotations in each field. These annotations tell HAMR how to sort and group beans into reducers. @SortField denote the field should be used to sort. The parameter level denotes the priority of that field. The higher the level, the higher the priority. In this [example][exampleCode], url has the highest priority in sorting, which means compare result of sessionId will have no effect unless two beans have the same url.On default, FieldComparator is used to compare fields. If you want to custom your field comparator, you can assign it as a parameter of @SortField. For example:
  
```java
    @SortField(comparator=yourComparator.class)
```
Note that your Comparator class must extend FieldComparator and returns only 1,0 or -1 in compare function.
  The field totalTime is used to collect count result and is not read from mapper input. Thus, there is no necessary to pass it from mapper to reducer. @SkipIO is used to denote that this field need not to be transferred.

  Reducer will receive these beans and pass them to Counters. Counters define what to do in reduce function. And different Counters may accomplish different jobs in reduce side. Thus, a reducer may contain several Counters. Counter classes are assigned by annotation @Counters on the subclasses of AnnotedBean. In this [example][exampleCode], there is only one counter that is written by user. The usage of some general Counters will be described in the next section.

  Lets have a look how Counter classes look like
  ```java
  public class VisitTimeCounter extends Counter {
	private Long firstTime;
	private Long lastTime;

	@SuppressWarnings("rawtypes")
	public VisitTimeCounter(Context context) {
		super(context);
	}

	@Override
	public void count(AnnotedBean ab) {
		Long time = ((VisitBean) ab).getMessageTime();
		if (firstTime == null) {
			firstTime = time;
		}
		lastTime = time;
	}

	@Override
	public boolean end(AnnotedBean ret) {
		if (firstTime != null && lastTime != null) {
			((VisitBean) ret).setTotalTime(lastTime - firstTime);
			return true;
		} else {
			return false;
		}
	}
    }
    ```
  There is two functions need to be rewritten in Counter class: count() and end(). Reducer will give every AnnotedBean from shuffle to each Counter by invoking their count function. At the end, Reducer will call end function of each Counter. The input of end function is the last non-null AnnotedBean from shuffle phrase. The returned boolean value tells the Reducer wether to write the result or not (counter can call context.write as the context is passed to counter in their construction method).If the result need not be write by reducer, the function end() should return false.
  Note that Counters are going to be a variable in reduce function, thus, the Counters will be built for each time reduce function is called. 

  As one shall suppose, there should be some class to generate these AnnotedBeans for HAMR. The @KeyGenerator is used to convert input key-value pair in map function to AnnotedBean. @KeyGenerator should point to a subclass of KeyGenerator. Here is the [example][exampleCode] KeyGenerator for visitTimeBean. The function generate() will gain the input of mapper and return a list of AnnotedBean.
  
```java
    public class VisitKeyGenerator extends KeyGenerator{
	@SuppressWarnings("rawtypes")
	public VisitKeyGenerator(Context context) {
		super(context);
	}
	public List<AnnotedBean> generate(Object keyin , Object valuein)
	{
		List<AnnotedBean> ret = new ArrayList<AnnotedBean>();
		String[] vals = valuein.toString().split("###");
		VisitBean bean = new VisitBean();
		bean.setMessageTime(Long.parseLong(vals[0]));
		bean.setSessionId(vals[1]);
		bean.setUrl(vals[2]);
		ret.add(bean);
		return ret;
	}
    }

  The last work is to assemble these things together into a job
  

    public class VisitTimeJob {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "visit time");
	    GeneralJob.generalization(VisitBean.class, job);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

    }
```
  Note, that if you want to custom your Reducer, you can set your own Reducer class to the job after generazation() is called.

  The [example][exampleCode] data is in examples/datas/visitdata.

##2.general counters

  To satisfy some common usage, HAMR provide some general Counters. 
###SumCounter
  SumCounter count the total number of AnnotedBean received by reduce function. @TargetField is used to tell SumCounter where to put the result. Here is the [example][exampleCode] of use SumCounter in classical MapReduce program word count.
  ```java
    @Counters(counters = {SumCounter.class})
    @Generator(keyGeneratorClass = WordKeyGenerator.class)
    public class WordCountBean extends AnnotedBean{
    	@GroupField
    	@SortField
    	public String word;
    	
    	@SkipIO
    	@TargetField(generateFrom = SumCounter.class)
    	private Integer count;
    	
    	public String toString()
    	{
    		return word + "###" + count;
    	}
    }
    ```
###SetSumCounter
SetSumCounter is used to count how many different value of a field presented in a reduce function. The ipcount in [example][exampleCode] code illustrate usage of this counter: you have got a web site's visitor data which contain a url and user's IP. And you want to know how many indentical IP had visited each url. This is how SetSumCounter is used in IpCountBean:
```java
    @Counters(counters = {SetSumCounter.class})
    public class IpCountBean extends AnnotedBean{
	@SortField
	@GroupField
	private String url;
	@ReduceField(counterClass = {SetSumCounter.class})
	private String ip;
	@SkipIO
	@TargetField(generateFrom = SetSumCounter.class ,     fromField = "ip")
	private Integer ipNum;
	}
```
When generateFrom assigned as SetSumCounter.class, fromField must be the name of the counting fields. And there should be @ReduceField on the counting fields.

##3.write a general counter
There are several functions provided in Counter that make writing a general counter easier. The maxtemprature in the [example][exampleCode] illustrated how to write a custom counter. Assume you have a file contains temperature of some cities in every day, and you need to calculate the max temperature of each city. As finding maximum number is a very common situation, it is worth to spend some time to write a general counter. 
```java
public class MaxTempratureCounter extends Counter{
	private Double MaxTemprature;
	@SuppressWarnings("rawtypes")
	public MaxTempratureCounter(Context context) {
		super(context);
		MaxTemprature = 0.0;
	}
	
	@Override
	public void count(AnnotedBean key) {
         Map<Field , ReduceField> allReduceField = key.getReduceField();
         Field tempratureField = allReduceField.keySet().iterator().next();
         try {
			Double temprature = (Double) tempratureField.get(key);
			if(temprature > MaxTemprature)
			{
				MaxTemprature = temprature;
			}
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public boolean end(AnnotedBean ret)
	{
		try {
			Map<Field , TargetField> allTargetField = getTargetFields(ret);
			allTargetField.keySet().iterator().next().set(ret, MaxTemprature);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return true;
	}
    }
```
There getReduceField() function will return a Map object containing fields which have annotation @ReduceField && counterClass is assigned to current counter class. The function getTargetFields() returns similiar to getReduceField(), but it returns the fields which have @TargetField pointing to current counter. 


4.preducer
//under construction


[exampleCode]:https://github.com/citymonkeymao/harm-examples

![tracking](http://getnotify.com/white.asp?eid=1483a959c3719abb)
