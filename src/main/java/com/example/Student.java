package com.example;

import java.util.Map;
import java.util.HashMap;

class Profile {
    public java.util.UUID id;
    public String name;
    public String preferences;
    public int chosen = 0;
    public Map<String,String> answers = new HashMap<String,String>();
}