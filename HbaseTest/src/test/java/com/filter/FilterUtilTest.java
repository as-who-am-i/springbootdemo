package com.filter;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class FilterUtilTest {

    @Test
    public void equalFilter() {
        try {
            FilterUtil.equalFilter("myTest","1", new String[]{"myfc1", "age"});
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void qualifierFilter() {
        try {
            FilterUtil.qualifierFilter("myTest",new String[]{"myfc1","sex"});
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void combineFilter()  {
        try {
            FilterUtil.combineFilter("myTest","1",new String[]{"myfc1","name"});
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}