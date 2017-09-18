package com.taxonic.rml.util;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collector;

public class CollectorUtils {
	
	public static <T> Collector<T, List<T>, T> toOnlyListItem(String message) {
        return Collector.of(
        		ArrayList::new,
        		List::add,
        		(left, right) -> { 
        			left.addAll(right); 
        			return left; 
        		},
        		l ->  {
        			if (l.size() != 1) {
        				throw new RuntimeException(message);
        			}
        			return l.get(0);
        		}
        );
    }

}
