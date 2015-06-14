package com.rajivnarula.aws.todo;

import java.util.* ;
import java.math.BigDecimal;

import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.PathVariable;

import org.springframework.web.context.request.WebRequest;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.auth.* ;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.* ;

import com.amazonaws.services.sqs.*;
import com.amazonaws.services.sqs.model.*;

@RestController
public class RequestController {

	static DynamoDB dynamoDB = new DynamoDB(new AmazonDynamoDBClient(new InstanceProfileCredentialsProvider()));

	@RequestMapping(value = "/processRequest", method = { RequestMethod.POST })
    public String processRequest(@RequestHeader("X-Aws-Sqsd-Queue") String queueName , @RequestBody String email) throws Exception{
		System.out.println ("processing...queueName:"+queueName);
		System.out.println ("processing...email:"+email);
		Set<String> taskList = getTaskList (email) ;
		System.out.println ("processing...taskList:"+taskList);
		AmazonSESSample.sendEmail(""+taskList,email);
		return "email sent to "+email +" taskList:"+taskList ;
    }

	private Set<String> getTaskList (String user){
		Table userTtable = dynamoDB.getTable("USER");
		Item userItem = null ;
		userItem = userTtable.getItem("user_id", user,  "task", null);
		Set<BigDecimal> currentTasks =null;
		if (userItem==null){
			throw new RuntimeException("User not found:"+user);
		}else{
			Set<String> listOfTasks = new HashSet<String>();
			currentTasks = userItem.getNumberSet("task");
			Table todoTable = dynamoDB.getTable("TODO");
			for (BigDecimal task :currentTasks){
				Item todoItem = todoTable.getItem("UUID", task,  "task", null);
				listOfTasks.add(todoItem.getString("task"));
			}
			return listOfTasks;
		}
	}


}