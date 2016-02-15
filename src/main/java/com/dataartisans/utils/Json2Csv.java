package com.dataartisans.utils;


import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class Json2Csv {

	public static void main(String[] args) throws IOException, ParseException {
		JSONParser p =  new JSONParser();
		Scanner sc = new Scanner(new FileInputStream(args[0]));
	//	FileOutputStream fos = new FileOutputStream(args[1]);

		// userId, time
		Map<Long, Map<Long, Integer>> map = new HashMap<>();
		while(sc.hasNextLine()) {
			JSONObject o = (JSONObject) p.parse(sc.nextLine());
			Long userId = Long.parseLong( o.get("userId").toString() );
			Long time = Long.parseLong( o.get("time").toString() );

			Map<Long, Integer> userWindows = map.get(userId);
			if(userWindows == null) {
				userWindows = new HashMap<>();
			}
			Long key = time / 60_000;
			Integer count = userWindows.get(key);
			if (count == null) {
				count = 1;
			} else {
				count++;
			}
			userWindows.put(key, count);
			map.put(userId, userWindows);
		}
		for(Map.Entry<Long, Map<Long, Integer>> userId: map.entrySet()) {
			System.out.println("User ID : " + userId.getKey() + " windows " + userId.getValue().size());
			for (Map.Entry<Long, Integer> winCount: userId.getValue().entrySet()) {
				System.out.println("Win start: " + winCount.getKey() + " count: " + winCount.getValue());
			}
		}
	//	fos.close();
		sc.close();

	}
}
