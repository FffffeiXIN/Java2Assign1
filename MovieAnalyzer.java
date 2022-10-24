package assignment1;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MovieAnalyzer {

    public static class Movie {
        private String Series_Title;
        private int Released_Year;
        private String Certificate;
        private String Runtime;
        private String Genre;
        private float IMDB_Rating;
        private String Overview;
        private String Meta_score;
        private String Director;
        private String Star1;
        private String Star2;
        private String Star3;
        private String Star4;
        private long Noofvotes;
        private String Gross;

        public Movie(String series_Title, int released_Year, String certificate, String runtime, String genre, float IMDB_Rating, String overview, String meta_score, String director, String star1, String star2, String star3, String star4, long noofvotes, String gross) {
            Series_Title = series_Title;
            Released_Year = released_Year;
            Certificate = certificate;
            Runtime = runtime;
            Genre = genre;
            this.IMDB_Rating = IMDB_Rating;
            Overview = overview;
            Meta_score = meta_score;
            Director = director;
            Star1 = star1;
            Star2 = star2;
            Star3 = star3;
            Star4 = star4;
            Noofvotes = noofvotes;
            Gross = gross;
        }

        public int getReleased_Year() {
            return Released_Year;
        }

        public String getGenre() {
            return Genre;
        }

        public void setGenre(String genre) {
            Genre = genre;
        }

        public String[] getOrderedStars() {
            String[] stars = new String[]{Star1, Star2, Star3, Star4};
            Arrays.sort(stars);
            return stars;
        }

        public int getRuntime() {
            return Integer.parseInt(Runtime.substring(0,Runtime.indexOf(" ")));
        }

        public int getLengthOfOverview() {
            return Overview.replaceAll("^\"|\"$", "").length();
        }

        public String getSeries_Title() {
            return Series_Title.replaceAll("^\"|\"$", "");
        }

        public float getIMDB_Rating() {
            return IMDB_Rating;
        }

        public String getGross() {
            return Gross;
        }

        @Override
        public String toString() {
            return "Movie{" +
                    "Series_Title='" + Series_Title + '\'' +
                    ", Released_Year=" + Released_Year +
                    ", Certificate='" + Certificate + '\'' +
                    ", Runtime='" + Runtime + '\'' +
                    ", Genre='" + Genre + '\'' +
                    ", IMDB_Rating=" + IMDB_Rating +
                    ", Overview='" + Overview + '\'' +
                    ", Meta_score=" + Meta_score +
                    ", Director='" + Director + '\'' +
                    ", Star1='" + Star1 + '\'' +
                    ", Star2='" + Star2 + '\'' +
                    ", Star3='" + Star3 + '\'' +
                    ", Star4='" + Star4 + '\'' +
                    ", Noofvotes=" + Noofvotes +
                    ", Gross=" + Gross +
                    '}';
        }
    }

    //    Stream<Movie> movieStream;
    Supplier<Stream<Movie>> movieSupplier;

    public MovieAnalyzer(String dataset_path){
        movieSupplier = () -> {
            try {
                return Files.lines(Paths.get(dataset_path))
                        .map(s -> s.substring(s.indexOf(".jpg") + 6))
                        .map(l -> l.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)", -1))
                        .map(a -> new Movie(a[0], Integer.parseInt(a[1]), a[2], a[3], a[4], Float.parseFloat(a[5]), a[6], a[7], a[8], a[9], a[10], a[11], a[12], Long.parseLong(a[13]), a[14]));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    public Map<Integer, Integer> getMovieCountByYear() {
        Map<Integer, Integer> res = new LinkedHashMap<>();
        movieSupplier.get().collect(Collectors.groupingBy(Movie::getReleased_Year, Collectors.counting()))
                .entrySet().stream().sorted(Map.Entry.<Integer, Long>comparingByKey().reversed())
                .forEachOrdered(e -> res.put(e.getKey(), e.getValue().intValue()));
        res.forEach((key, value) -> System.out.println(key + " == " + value));
        return res;
    }

    public Map<String, Integer> getMovieCountByGenre(){
        Map<String, Integer> temp = new LinkedHashMap<>();
        Map<String, Integer> res = new LinkedHashMap<>();
        movieSupplier.get().forEach(m->{
            m.setGenre(m.getGenre().replace("\"",""));
            String[] genres = m.getGenre().split(", ");
            for (int i = 0; i < genres.length; i++) {
                temp.merge(genres[i],1, (o,n)->o+1);
            }
        });
        temp.entrySet().stream().sorted(Map.Entry.<String,Integer>comparingByValue().reversed().thenComparing(Map.Entry::getKey))
                .forEachOrdered(e -> res.put(e.getKey(),e.getValue()));

        res.forEach((key, value) -> System.out.println(key + " == " + value));
        return res;
    }

    public Map<List<String>, Integer> getCoStarCount(){
        Map<List<String>, Integer> res = new LinkedHashMap<>();

        movieSupplier.get().forEach(m->{
            String[] stars = m.getOrderedStars();
            res.merge(Arrays.asList(stars[0],stars[1]),1,(o,n)->o+1);
            res.merge(Arrays.asList(stars[0],stars[2]),1,(o,n)->o+1);
            res.merge(Arrays.asList(stars[0],stars[3]),1,(o,n)->o+1);
            res.merge(Arrays.asList(stars[1],stars[2]),1,(o,n)->o+1);
            res.merge(Arrays.asList(stars[1],stars[3]),1,(o,n)->o+1);
            res.merge(Arrays.asList(stars[2],stars[3]),1,(o,n)->o+1);
        });

        res.forEach((key, value) -> System.out.println(key.toString() + " == " + value));
        return res;
    }

    public List<String> getTopMovies(int top_k, String by){
        List<String> res = new LinkedList<>();
        if(by.equals("runtime")){
            movieSupplier.get().sorted(Comparator.comparing(Movie::getRuntime).reversed().thenComparing(Movie::getSeries_Title))
                    .limit(top_k).forEachOrdered(e-> res.add(e.getSeries_Title()));
        }else if(by.equals("overview")){
            movieSupplier.get().sorted(Comparator.comparingInt(Movie::getLengthOfOverview).reversed().thenComparing(Movie::getSeries_Title))
                    .limit(top_k).forEachOrdered(e-> res.add(e.getSeries_Title()));
        }
        res.forEach(System.out::println);
        return res;
    }

    public List<String> getTopStars(int top_k, String by){
        class Pair1{
            String name;
            float rating;

            public Pair1(String name, float rating) {
                this.name = name;
                this.rating = rating;
            }

            public String getName() {
                return name;
            }

            public float getRating() {
                return rating;
            }
        }
        class Pair2{
            public String name;
            public long gross;

            public Pair2(String name, long gross) {
                this.name = name;
                this.gross = gross;
            }

            public String getName() {
                return name;
            }

            public long getGross() {
                return gross;
            }
        }
        List<String> res = new LinkedList<>();
        List<Pair1> temp1 = new ArrayList<>();
        List<Pair2> temp2 = new ArrayList<>();

        if(by.equals("rating")){
            movieSupplier.get().forEach(e->{
                float rating = e.getIMDB_Rating();
                String[] stars = e.getOrderedStars();
                temp1.add(new Pair1(stars[0],rating));
                temp1.add(new Pair1(stars[1],rating));
                temp1.add(new Pair1(stars[2],rating));
                temp1.add(new Pair1(stars[3],rating));
            });
            temp1.stream().collect(Collectors.groupingBy(Pair1::getName,Collectors.averagingDouble(Pair1::getRating)))
                    .entrySet().stream().sorted(Map.Entry.<String,Double>comparingByValue().reversed().thenComparing(Map.Entry::getKey))
                    .limit(top_k).forEachOrdered(e -> res.add(e.getKey()));
        }
        else if(by.equals("gross")){
            movieSupplier.get().filter(e->!e.getGross().equals("")).forEach(e->{
                long gross = Long.parseLong(e.getGross().replace("\"","").replace(",",""));
                String[] stars = e.getOrderedStars();
                temp2.add(new Pair2(stars[0],gross));
                temp2.add(new Pair2(stars[1],gross));
                temp2.add(new Pair2(stars[2],gross));
                temp2.add(new Pair2(stars[3],gross));
            });
            temp2.stream().collect(Collectors.groupingBy(Pair2::getName,Collectors.averagingDouble(Pair2::getGross)))
                    .entrySet().stream().sorted(Map.Entry.<String,Double>comparingByValue().reversed().thenComparing(Map.Entry::getKey))
                    .limit(top_k).forEachOrdered(e -> res.add(e.getKey()));
        }
        res.forEach(System.out::println);
        return res;
    }

    public List<String> searchMovies(String genre, float min_rating, int max_runtime){
//        List<String> res = new LinkedList<>();
        List<String> res = movieSupplier.get().filter(e -> e.getGenre().contains(genre))
                .filter(e -> e.getIMDB_Rating() >= min_rating)
                .filter(e -> e.getRuntime() <= max_runtime)
                .map(Movie::getSeries_Title).sorted().toList();
        res.forEach(System.out::println);
        return res;
    }


//    public static void main(String[] args) throws IOException {
//        MovieAnalyzer movie = new MovieAnalyzer("E:\\大三上\\Java2\\A1_Sample\\resources\\imdb_top_500.csv");
////        movie.getMovieCountByYear();
////        movie.getMovieCountByGenre();
////        movie.getCoStarCount();
////        movie.getTopMovies(100,"overview");
////        movie.getTopStars(80,"gross");
//        movie.searchMovies("Action", 7.7f, 200);
//
////        String str = "skldnfcaj,envjksvn";
////        String[] a = str.split(",");
////        for (int i = 0; i < a.length; i++) {
////            System.out.println(a[i]);
////        }
//    }
}