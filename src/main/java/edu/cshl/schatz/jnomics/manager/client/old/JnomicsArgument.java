package edu.cshl.schatz.jnomics.manager.client.old;

/**
 * User: james
 */
public class JnomicsArgument {

    private String name;
    private boolean required;
    private boolean hasArguments;
    private String description;

    public JnomicsArgument(String name, boolean required,
                           boolean hasArguments, String description){
        this.name = name;
        this.required = required;
        this.hasArguments = hasArguments;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }

    public boolean hasArguments() {
        return hasArguments;
    }

    public void setHasArguments(boolean hasArg) {
        this.hasArguments = hasArg;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
