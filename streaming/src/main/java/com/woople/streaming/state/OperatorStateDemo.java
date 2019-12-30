package com.woople.streaming.state;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class OperatorStateDemo {
    public static void main(String[] args) {
        Optional<Integer> optional = Optional.of(1);

        Map<String, Boolean> map = new HashMap<>();

        System.out.println(Optional.ofNullable(map.get("aa")).orElse(false));



    }
}
