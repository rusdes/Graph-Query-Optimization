package ldbc;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import ldbc.group.*;
import ldbc.join.*;
import ldbc.map.*;
import operators.datastructures.*;

// import org.apache.flink.api.java.List;
import org.javatuples.*;
// import org.apache.flink.api.java.ExecutionEnvironment;
// import org.apache.flink.api.java.tuple.Decade;
// import org.apache.flink.api.java.tuple.Triplet;
// import org.apache.flink.api.java.tuple.Quartet;
// import org.apache.flink.api.java.tuple.Sextet;
// import org.apache.flink.api.java.tuple.Octet;
// import org.apache.flink.api.java.tuple.Ennead;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import org.apache.flink.core.fs.FileSystem;

/*
* Convert all CSV files generated by LDBC-SNB into a labeled property graph data model
* */

public class LDBCToGraphDataModel {
	private String dir;
	// private ExecutionEnvironment env;
	public LDBCToGraphDataModel(String dir) {
		
		this.dir = dir;
		// this.env = env;
	}
	
	public void getGraph() throws Exception {
		//Read and extract all vertices with label comment, store them into tuples
		List<Sextet<Long, String, String, String, String, String>> comments = new ArrayList<>();
		try (Reader reader = Files.newBufferedReader(Paths.get(dir ,"comment_0_0.csv"))) {
			try (CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(0)
																   .withCSVParser(new CSVParserBuilder().withSeparator('|').build())
																   .build()) {
				String[] line;
				line = csvReader.readNext(); //Skip first line 
				while ((line = csvReader.readNext()) != null) {
					Sextet<Long, String, String, String, String, String> holder = new Sextet<Long, String, String, String, String, String>
																							(Long.parseLong(line[0]), line[1], line[2], line[3], 
																							line[4], line[5]);
					comments.add(holder);
				}
			}
		}
		final String[] commentItems = {"comment", "id", "creationDate", "locationIP", "browserUsed", "content", "length"};


		//Read and extract all vertices with label forum, store them into tuples
		List<Triplet<Long, String, String>> forums = new ArrayList<>();
		try (Reader reader = Files.newBufferedReader(Paths.get(dir ,"forum_0_0.csv"))) {
			try (CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(0)
																   .withCSVParser(new CSVParserBuilder().withSeparator('|').build())
																   .build()) {
				String[] line;
				line = csvReader.readNext(); //Skip first line 
				while ((line = csvReader.readNext()) != null) {
					Triplet<Long, String, String> holder = new Triplet<Long, String, String>(Long.parseLong(line[0]), line[1], line[2]);
					forums.add(holder);
				}
			}
		}
		final String[] forumItems = {"forum", "id", "title", "creationDate"};


		//Read and extract all vertices with label tag, store them into tuples
		List<Triplet<Long, String, String>> tags = new ArrayList<>();
		try (Reader reader = Files.newBufferedReader(Paths.get(dir ,"tag_0_0.csv"))) {
			try (CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(0)
																   .withCSVParser(new CSVParserBuilder().withSeparator('|').build())
																   .build()) {
				String[] line;
				line = csvReader.readNext(); //Skip first line 
				while ((line = csvReader.readNext()) != null) {
					Triplet<Long, String, String> holder = new Triplet<Long, String, String>(Long.parseLong(line[0]), line[1], line[2]);
					tags.add(holder);
				}
			}
		}
		final String[] tagItems = {"tag", "id", "name", "url"};

		//Read and extract all vertices with label tagclass, store them into tuples
		List<Triplet<Long, String, String>> tagclasses = new ArrayList<>();
		try (Reader reader = Files.newBufferedReader(Paths.get(dir ,"tagclass_0_0.csv"))) {
			try (CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(0)
																   .withCSVParser(new CSVParserBuilder().withSeparator('|').build())
																   .build()) {
				String[] line;
				line = csvReader.readNext(); //Skip first line 
				while ((line = csvReader.readNext()) != null) {
					Triplet<Long, String, String> holder = new Triplet<Long, String, String>(Long.parseLong(line[0]), line[1], line[2]);
					tagclasses.add(holder);
				}
			}
		}
		final String[] tagclassItems = {"tagclass", "id", "name", "url"};

		//Read and extract all vertices with label post, store them into tuples
		List<Octet<Long, String, String, String, String, String, String, String>> posts = new ArrayList<>();
		try (Reader reader = Files.newBufferedReader(Paths.get(dir ,"post_0_0.csv"))) {
			try (CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(0)
																   .withCSVParser(new CSVParserBuilder().withSeparator('|').build())
																   .build()) {
				String[] line;
				line = csvReader.readNext(); //Skip first line 
				while ((line = csvReader.readNext()) != null) {
					Octet<Long, String, String, String, String, String, String, String> holder = new Octet<Long, String, String, String, String, String, String, String>
																									 (Long.parseLong(line[0]), line[1], line[2], line[3], line[4],
																									  line[5], line[6], line[7]);
					posts.add(holder);
				}
			}
		}
		final String[] postItems = {"post", "id", "imageFile", "creationDate", "locationIP", "browserUsed", "language", "content", "length"};

		//Read and extract all vertices with label place, store them into tuples
		List<Quartet<Long, String, String, String>> places = new ArrayList<>();
		try (Reader reader = Files.newBufferedReader(Paths.get(dir ,"place_0_0.csv"))) {
			try (CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(0)
																   .withCSVParser(new CSVParserBuilder().withSeparator('|').build())
																   .build()) {
				String[] line;
				line = csvReader.readNext(); //Skip first line 
				while ((line = csvReader.readNext()) != null) {
					Quartet<Long, String, String, String> holder = new Quartet<Long, String, String, String>
																									 (Long.parseLong(line[0]), line[1], line[2], line[3]);
					places.add(holder);
				}
			}
		}
		final String[] placeItems = {"place", "id", "name", "url", "type"};

		//Read and extract all vertices with label organisation, store them into tuples
		List<Quartet<Long, String, String, String>> organisation = new ArrayList<>();
		try (Reader reader = Files.newBufferedReader(Paths.get(dir ,"organisation_0_0.csv"))) {
			try (CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(0)
																   .withCSVParser(new CSVParserBuilder().withSeparator('|').build())
																   .build()) {
				String[] line;
				line = csvReader.readNext(); //Skip first line 
				while ((line = csvReader.readNext()) != null) {
					Quartet<Long, String, String, String> holder = new Quartet<Long, String, String, String>
																									 (Long.parseLong(line[0]), line[1], line[2], line[3]);
					organisation.add(holder);
				}
			}
		}
		final String[] organisationItems = {"organisation", "id", "type", "name", "url"};

		//Read and extract all vertices with label person, store them into tuples
		List<Octet<Long, String, String, String, String, String, String, String>> person = new ArrayList<>();
		try (Reader reader = Files.newBufferedReader(Paths.get(dir ,"person_0_0.csv"))) {
			try (CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(0)
																   .withCSVParser(new CSVParserBuilder().withSeparator('|').build())
																   .build()) {
				String[] line;
				line = csvReader.readNext(); //Skip first line 
				while ((line = csvReader.readNext()) != null) {
					Octet<Long, String, String, String, String, String, String, String> holder = new Octet<Long, String, String, String, String, String, String, String>
																									 (Long.parseLong(line[0]), line[1], line[2], line[3], line[4],
																									  line[5], line[6], line[7]);
					person.add(holder);
				}
			}
		}
		
		//Add the property "email" to tuples with label person
		List<Ennead<Long, String, String, String, String, String, String, String, String>> personWithEmail = env.readCsvFile(dir + "person_email_emailaddress_0_0.csv")
				.ignoreFirstLine()
				.fieldDelimiter("|")
				.types(Long.class, String.class)
				.coGroup(person)
				.where(0)
				.equalTo(0)
				.with(new PersonGroupEmail());

		//Add the property "language" to tuples with label person
		List<Decade<Long, String, String, String, String, String, String, String, String, String>> personWithEmailAndLanguage = env.readCsvFile(dir + "person_speaks_language_0_0.csv")
				.ignoreFirstLine()
				.fieldDelimiter("|")
				.types(Long.class, String.class)
				.coGroup(personWithEmail)
				.where(0)
				.equalTo(0)
				.with(new PersonGroupLanguage());
		
		final String[] personItems = {"person", "id", "firstName", "lastName", "gender", "birthday", "creationDate", "locationIP", "browserUsed", "email", "language"};


		//Assign unique IDs to all vertex tuples
		List<Quartet<Long, HashSet<String>, HashMap<String, String>, Long>> commentVertices = comments.map(new CommentMap(commentItems)); 
		
		List<Quartet<Long, HashSet<String>, HashMap<String, String>, Long>> commentMaxId = commentVertices.max(0);
		
		List<Quartet<Long, HashSet<String>, HashMap<String, String>, Long>> forumVertices = commentMaxId.cross(forums).with(new ForumMap(forumItems));
		
		List<Quartet<Long, HashSet<String>, HashMap<String, String>, Long>> forumMaxId = forumVertices.max(0);
		
		List<Quartet<Long, HashSet<String>, HashMap<String, String>, Long>> tagVertices = forumMaxId.cross(tags).with(new TagMap(tagItems));
		
		List<Quartet<Long, HashSet<String>, HashMap<String, String>, Long>> tagMaxId = tagVertices.max(0);
		
		List<Quartet<Long, HashSet<String>, HashMap<String, String>, Long>> tagclassVertices = tagMaxId.cross(tagclasses).with(new TagClassMap(tagclassItems));
		
		List<Quartet<Long, HashSet<String>, HashMap<String, String>, Long>> tagclassMaxId = tagclassVertices.max(0);
		
		List<Quartet<Long, HashSet<String>, HashMap<String, String>, Long>> postVertices = tagclassMaxId.cross(posts).with(new PostMap(postItems));

		List<Quartet<Long, HashSet<String>, HashMap<String, String>, Long>> postMaxId = postVertices.max(0);
		
		List<Quartet<Long, HashSet<String>, HashMap<String, String>, Long>> placeVertices = postMaxId.cross(places).with(new PlaceMap(placeItems));

		List<Quartet<Long, HashSet<String>, HashMap<String, String>, Long>> placeMaxId = placeVertices.max(0);
		
		List<Quartet<Long, HashSet<String>, HashMap<String, String>, Long>> organisationVertices = placeMaxId.cross(organisation)
																											 .with(new OrganisationMap(organisationItems));

		List<Quartet<Long, HashSet<String>, HashMap<String, String>, Long>> organisationMaxId = organisationVertices.max(0);

		List<Quartet<Long, HashSet<String>, HashMap<String, String>, Long>> personVertices = organisationMaxId.cross(personWithEmailAndLanguage)
																											  .with(new PersonMap(personItems));
		
		List<Quartet<Long, HashSet<String>, HashMap<String, String>, Long>> personMaxId = personVertices.max(0);

		//Merge tuples into a List and convert them into vertices
		List<VertexExtended<Long, HashSet<String>, HashMap<String, String>>> vertices = commentVertices.union(forumVertices)
																									   .union(tagVertices)
																									   .union(tagclassVertices)
																									   .union(placeVertices)
																									   .union(organisationVertices)
																									   .union(postVertices)
																									   .union(personVertices)
																									   .map(new DeleteOriginalId());

		//Return source vertex IDs with edge IDs
		EdgeIdReplacerLeft getSourceIds = new EdgeIdReplacerLeft();
		//Retur edge IDs with target IDs
		EdgeIdReplacerRight getTargetIds = new EdgeIdReplacerRight();
		
		//Extract edges with label hasCreater between source vertices with label comment and a target vertices with label person, also assign unique IDs to edges
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasCreatorFromComment = env.readCsvFile(dir + "comment_hasCreator_person_0_0.csv")
																									.ignoreFirstLine()
																									.fieldDelimiter("|")
																									.types(Long.class, Long.class)
																									.join(commentVertices)
																									.where(0)
																									.equalTo(3)
																									.with(getSourceIds)
																									.join(personVertices)
																									.where(1)
																									.equalTo(3)
																									.with(getTargetIds)
																									.cross(personMaxId)
																									.with(new HasCreatorMap("hasCreator"));
		
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasCreatorFromCommentMaxId = hasCreatorFromComment.max(0);

		//Extract edges with label hasTag between source vertices with label comment and target vertices with label tag, also assign unique IDs to edges
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasTagFromComment = getEdges(dir + "comment_hasTag_tag_0_0.csv", 
																									 commentVertices,
																									 tagVertices,
																									 hasCreatorFromCommentMaxId,
																							  "hasTag");
		
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasTagFromCommentMaxId = hasTagFromComment.max(0);

		//Extract edges with label isLocatedIn between source vertices with label comment and target vertices with label place, also assign unique IDs to edges
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> isLocatedInFromComment =
				getEdges(dir + "comment_isLocatedIn_place_0_0.csv", 
						commentVertices,
						placeVertices,
						hasTagFromCommentMaxId,
						"isLocatedIn");
				
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> isLocatedInFromCommentMaxId = isLocatedInFromComment.max(0);

		//Extract edges with label replayOf between source vertices with label comment and target vertices with label comment, also assign unique IDs to edges
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> replyOfComment =
				getEdges(dir + "comment_replyOf_comment_0_0.csv", 
						commentVertices,
						commentVertices,
						isLocatedInFromCommentMaxId,
						"replyOf");

		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> replyOfCommentMaxId = replyOfComment.max(0);

		//Extract edges with label replayOf between source vertices with label comment and target vertices with label post, also assign unique IDs to edges
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> replyOfPost =
				getEdges(dir + "comment_replyOf_post_0_0.csv", 
						commentVertices,
						postVertices,
						replyOfCommentMaxId,
						"replyOf");

		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> replyOfPostMaxId = replyOfPost.max(0);

		//Extract edges with label containerOf between source vertices with label forum and target vertices with label post, also assign unique IDs to edges
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> containerOf =
				getEdges(dir + "forum_containerOf_post_0_0.csv", 
						forumVertices,
						postVertices,
						replyOfPostMaxId,
						"containerOf");

		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> containerOfMaxId = containerOf.max(0);

		//Extract edges with label hasMember between source vertices with label forum and target vertices with label person, also assign unique IDs to edges
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasMember = getEdgesWithThreeElements(
				dir + "forum_hasMember_person_0_0.csv",
				forumVertices,
				personVertices,
				containerOfMaxId,
				"hasMember",
				"joinDate");
		
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasMemberMaxId = hasMember.max(0);

		//Extract edges with label hasModerator between source vertices with label forum and target vertices with label person, also assign unique IDs to edges
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasModerator =
				getEdges(dir + "forum_hasModerator_person_0_0.csv", 
						forumVertices,
						personVertices,
						hasMemberMaxId,
						"hasModerator");

		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasModeratorMaxId = hasModerator.max(0);

		//Extract edges with label hasTag between source vertices with label forum and target vertices with label tag, also assign unique IDs to edges
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasTagFromForum =
				getEdges(dir + "forum_hasTag_tag_0_0.csv", 
						forumVertices,
						tagVertices,
						hasModeratorMaxId ,
						"hasTag");

		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasTagFromForumMaxId = hasTagFromForum.max(0);

		//Extract edges with label isLocatedIn between source vertices with label organisation and target vertices with label place, also assign unique IDs to edges
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> isLocatedInFromOrganisation =
				getEdges(dir + "organisation_isLocatedIn_place_0_0.csv", 
						organisationVertices,
						placeVertices,
						hasTagFromForumMaxId,
						"isLocatedIn");
		
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> isLocatedFromOrganisationMaxId = isLocatedInFromOrganisation.max(0);

		//Extract edges with label hasInterest between source vertices with label person and target vertices with label tag, also assign unique IDs to edges
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasInterest =
				getEdges(dir + "person_hasInterest_tag_0_0.csv", 
						personVertices,
						tagVertices,
						isLocatedFromOrganisationMaxId,
						"hasInterest");
		
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasInterestMaxId = hasInterest.max(0);

		//Extract edges with label isLocatedIn between source vertices with label person and target vertices with label place, also assign unique IDs to edges
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> isLocatedInFromPerson =
				getEdges(dir + "person_isLocatedIn_place_0_0.csv", 
						personVertices,
						placeVertices,
						hasInterestMaxId,
						"isLocatedIn");
		
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> isLocatedInFromPersonMaxId = isLocatedInFromPerson.max(0);

		//Extract edges with label knows between source vertices with label person and target vertices with label person, also assign unique IDs to edges
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> knows = getEdgesWithThreeElements(
				dir + "person_knows_person_0_0.csv",
				personVertices,
				personVertices,
				isLocatedInFromPersonMaxId,
				"knows",
				"creationDate");

		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> knowsMaxId = knows.max(0);

		//Extract edges with label likes between source vertices with label person and target vertices with label comment, also assign unique IDs to edges
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> likesComment = getEdgesWithThreeElements(
				dir + "person_likes_comment_0_0.csv",
				personVertices,
				commentVertices,
				knowsMaxId,
				"likes",
				"creationDate");

		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> likesCommentMaxId = likesComment.max(0);

		//Extract edges with label likes between source vertices with label person and target vertices with label post, also assign unique IDs to edges
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> likesPost = getEdgesWithThreeElements(
				dir + "person_likes_post_0_0.csv",
				personVertices,
				postVertices,
				likesCommentMaxId,
				"likes",
				"creationDate");

		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> likesPostMaxId = likesPost.max(0);

		//Extract edges with label studyAt between source vertices with label person and target vertices with label organisation, also assign unique IDs to edges
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> studyAt = getEdgesWithThreeElements(
				dir + "person_studyAt_organisation_0_0.csv",
				personVertices,
				organisationVertices,
				likesPostMaxId,
				"studyAt",
				"classYear");

		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> studyAtMaxId = studyAt.max(0);

		//Extract edges with label workAt between source vertices with label person and target vertices with label organisation, also assign unique IDs to edges
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> workAt = getEdgesWithThreeElements(
				dir + "person_workAt_organisation_0_0.csv",
				personVertices,
				organisationVertices,
				studyAtMaxId,
				"workAt",
				"workFrom");

		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> workAtMaxId = workAt.max(0);

		//Extract edges with label isPartOf between source vertices with label place and target vertices with label place, also assign unique IDs to edges
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> isPartOf =
				getEdges(dir + "place_isPartOf_place_0_0.csv", 
						placeVertices,
						placeVertices,
						workAtMaxId,
						"isPartOf");

		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> isPartOfMaxId = isPartOf.max(0);

		//Extract edges with label hasCreator between source vertices with label post and target vertices with label person, also assign unique IDs to edges
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasCreatorFromPost =
				getEdges(dir + "post_hasCreator_person_0_0.csv", 
						postVertices,
						personVertices,
						isPartOfMaxId,
						"hasCreator");

		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasCreatorFromPostMaxId = hasCreatorFromPost.max(0);

		//Extract edges with label hasTag between source vertices with label post and target vertices with label tag, also assign unique IDs to edges
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasTagFromPost =
				getEdges(dir + "post_hasTag_tag_0_0.csv", 
						postVertices,
						tagVertices,
						hasCreatorFromPostMaxId,
						"hasCreator");

		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasTagFromPostMaxId = hasTagFromPost.max(0);

		//Extract edges with label isLocatedIn between source vertices with label post and target vertices with label place, also assign unique IDs to edges
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> isLocatedInFromPost =
				getEdges(dir + "post_isLocatedIn_place_0_0.csv", 
						postVertices,
						placeVertices,
						hasTagFromPostMaxId,
						"isLocatedIn");

		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> isLocatedInFromPostMaxId = isLocatedInFromPost.max(0);

		//Extract edges with label hasType between source vertices with label tag and target vertices with label tagclass, also assign unique IDs to edges
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasType =
				getEdges(dir + "tag_hasType_tagclass_0_0.csv", 
						tagVertices,
						tagclassVertices,
						isLocatedInFromPostMaxId,
						"hasType");

		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> hasTypeMaxId = hasType.max(0);

		//Extract edges with label isSubclassOf between source vertices with label tagclass and target vertices with label tagclass, also assign unique IDs to edges
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> isSubclassOf =
				getEdges(dir + "tagclass_isSubclassOf_tagclass_0_0.csv", 
						tagclassVertices,
						tagclassVertices,
						hasTypeMaxId,
						"isSubclassOf");		

		//Merge all edges in a List
		List<EdgeExtended<Long, Long, String, HashMap<String, String>>> edges = hasCreatorFromComment
				.union(hasTagFromComment)
				.union(isLocatedInFromComment)
				.union(replyOfComment)
				.union(replyOfPost)
				.union(containerOf)
				.union(hasMember)
				.union(hasModerator)
				.union(hasTagFromForum)
				.union(isLocatedInFromOrganisation)
				.union(hasInterest)
				.union(isLocatedInFromPerson)
				.union(knows)
				.union(likesPost)
				.union(likesComment)
				.union(studyAt)
				.union(workAt)
				.union(isPartOf)
				.union(hasCreatorFromPost)
				.union(hasTagFromPost)
				.union(isLocatedInFromPost)
				.union(hasType)
				.union(isSubclassOf);

		//Write the vertices and the edges into CSV files for further use
		vertices.writeAsCsv(dir + "vertices.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);
		edges.writeAsCsv(dir + "edges.csv", "\n", "|", FileSystem.WriteMode.OVERWRITE);

		//Set the parallelism level of Flink to 1 for generating unique IDs, other number will introduce duplicated IDs
		// env.setParallelism(1);
		//Start execution
		// env.execute();
	}

	//Generate edges by joining edges with source and target vertices, also assign unique IDs to edges
	private List<EdgeExtended<Long, Long, String, HashMap<String, String>>> getEdges(
			String path,
			List<Quartet<Long, HashSet<String>, HashMap<String, String>, Long>> verticesLeft, 
			List<Quartet<Long, HashSet<String>, HashMap<String, String>, Long>> verticesRight,
			List<EdgeExtended<Long, Long, String, HashMap<String, String>>> lastMaxId,
			String label) {
		EdgeIdReplacerLeft getSourceIds = new EdgeIdReplacerLeft();
		EdgeIdReplacerRight getTargetIds = new EdgeIdReplacerRight();
		return env.readCsvFile(path)
				.ignoreFirstLine()
				.fieldDelimiter("|")
				.types(Long.class, Long.class)
				.join(verticesLeft)
				.where(0)
				.equalTo(3)
				.with(getSourceIds)
				.join(verticesRight)
				.where(1)
				.equalTo(3)
				.with(getTargetIds)
				.cross(lastMaxId)
				.with(new EdgeWithTwoElementsMap(label));
		
	}

	//Generate edges by joining edges with source and target vertices, also assign unique IDs to edges
	private List<EdgeExtended<Long, Long, String, HashMap<String, String>>> getEdgesWithThreeElements(
			String path,
			List<Quartet<Long, HashSet<String>, HashMap<String, String>, Long>> verticesLeft, 
			List<Quartet<Long, HashSet<String>, HashMap<String, String>, Long>> verticesRight,
			List<EdgeExtended<Long, Long, String, HashMap<String, String>>> lastMaxId,
			String label,
			String key) {
		EdgeIdReplacerLeftThreeElements getSourceIds = new EdgeIdReplacerLeftThreeElements();
		EdgeIdReplacerRightThreeElements getTargetIds = new EdgeIdReplacerRightThreeElements();
		return env.readCsvFile(path)
				.ignoreFirstLine()
				.fieldDelimiter("|")
				.types(Long.class, Long.class, String.class)
				.join(verticesLeft)
				.where(0)
				.equalTo(3)
				.with(getSourceIds)
				.join(verticesRight)
				.where(1)
				.equalTo(3)
				.with(getTargetIds)
				.cross(lastMaxId)
				.with(new EdgeWithThreeElementsMap(label, key));
		
	}
}
