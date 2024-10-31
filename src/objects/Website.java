package objects;

public class Website {
    String url, title, description;
    public Website(String url, String title, String description) {
        this.url = url;
        this.title = title;
        this.description = description;
    }
    public String getUrl() {
        return url;
    }
    public String getTitle() {
        return title;
    }
    public String getDescription() {
        return description;
    }
}
