package com.e8.search.util;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;

public class RandomDataGenerator {
    private static String randomIpAddress(){
        Random rand = new Random();
        return String.format("%d.%d.%d.%d", rand.nextInt(256), rand.nextInt(256), rand.nextInt(256), rand.nextInt(256));
    }
    private static String randomMACAddress(){
        Random rand = new Random();
        byte[] macAddr = new byte[6];
        rand.nextBytes(macAddr);

        macAddr[0] = (byte)(macAddr[0] & (byte)254);

        StringBuilder sb = new StringBuilder(18);
        for(byte b : macAddr){
            if(sb.length() > 0)
                sb.append(":");

            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
    private static String randomHostName(){
       return  RandomStringUtils.randomAlphanumeric(10);
    }
    private static String randomUserName(){
        return RandomStringUtils.randomAlphanumeric(10);
    }
    public static void csv(OutputStream outputStream, long num) throws Exception{
        String header = "ipAddress,hostName,macAddress,userName\n";
        outputStream.write(header.getBytes(StandardCharsets.UTF_8));
        for(long i=0; i<num; i++){
            String builder = randomIpAddress() +
                    "," +
                    randomHostName() +
                    "," +
                    randomMACAddress() +
                    "," +
                    randomUserName() +
                    "\n";
            outputStream.write(builder.getBytes(StandardCharsets.UTF_8));
        }
    }
    public static void main(String[] args) throws Exception{
        csv(new FileOutputStream("data_10M.csv"), 10000000);
    }
}
