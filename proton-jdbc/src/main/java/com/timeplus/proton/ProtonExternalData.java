package com.timeplus.proton;

import java.io.InputStream;

/**
 * @author zgmnkv
 */
public class ProtonExternalData {

    private String name;
    private InputStream content;
    private String format;
    private String types;
    private String structure;

    public ProtonExternalData() {
    }

    public ProtonExternalData(String name, InputStream content) {
        this.name = name;
        this.content = content;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public InputStream getContent() {
        return content;
    }

    public void setContent(InputStream content) {
        this.content = content;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getTypes() {
        return types;
    }

    public void setTypes(String types) {
        this.types = types;
    }

    public String getStructure() {
        return structure;
    }

    public void setStructure(String structure) {
        this.structure = structure;
    }

    public ProtonExternalData withName(String name) {
        this.name = name;
        return this;
    }

    public ProtonExternalData withContent(InputStream content) {
        this.content = content;
        return this;
    }

    public ProtonExternalData withFormat(String format) {
        this.format = format;
        return this;
    }

    public ProtonExternalData withTypes(String types) {
        this.types = types;
        return this;
    }

    public ProtonExternalData withStructure(String structure) {
        this.structure = structure;
        return this;
    }

}
