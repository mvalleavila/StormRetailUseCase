package org.buildoop.storm.tools;

import java.util.Properties;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class SendEmail {
	
	/**
	 * Sents an email to the distributor 
	 * @throws MessagingException 
	 */
	//public static void sendEMailToDistributor(String shopName, String product, int stockMin, int stockRemaining)
	public static void sendEMailToDistributor()
	{		
		/*TODO: make configurable values for:
		 * mailDirection
		 * 
		 */
		
		//String mailAdress
		
		Properties props = new Properties();
		props.put("mail.smtp.host", "smtp.gmail.com");
		props.put("mail.transport.protocol", "smtps");
		
		Session session = Session.getInstance(props);
		Message msg = new MimeMessage(session);
		Transport t = null;
		
		try{
			//Address mail = new InternetAddress(mailAdress);
			Address mail = new InternetAddress("mvalleavila@gmail.com");
			msg.setFrom(mail);
			msg.setRecipient(Message.RecipientType.TO, mail);
			msg.setSubject("Asunto de prueba");
			msg.setText("Cuerpo de prueba");
			
			Transport.send(msg, "mvalleavila", "password");
		} catch (MessagingException ex){
			ex.printStackTrace();
		}
	}
	
	public static void main(String[] args){
		sendEMailToDistributor();
	}
}
