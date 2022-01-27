package com.example;

import java.util.Map;
import java.util.HashMap;

class Student {
    public java.util.UUID id;
    public String name;
    public String schoolClass;
    public String gender;
    public String genderPreference;
    public String address;
    public String preferences;
    public int chosen = 0;
    public Map<String,String> answers = new HashMap<String,String>();
}