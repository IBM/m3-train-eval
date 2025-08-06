

from fastapi import APIRouter, Query
import sqlite3

app = APIRouter()

# Database connection
conn = sqlite3.connect('invocable_api_hub/db/bird/test/codebase_community.sqlite', check_same_thread=False)
cursor = conn.cursor()

# Endpoint to get users with specific display names and maximum reputation
@app.get("/v1/bird/codebase_community/users/max_reputation", summary="Get users with specific display names and maximum reputation")
async def get_users_max_reputation(display_names: str = Query(..., description="Comma-separated list of display names")):
    display_names_list = display_names.split(',')
    query = """
    SELECT DisplayName FROM users
    WHERE DisplayName IN ({})
    AND Reputation = (
        SELECT MAX(Reputation) FROM users
        WHERE DisplayName IN ({})
    )
    """.format(','.join(['?'] * len(display_names_list)), ','.join(['?'] * len(display_names_list)))
    cursor.execute(query, display_names_list * 2)
    return cursor.fetchall()

# Endpoint to get users created in a specific year
@app.get("/v1/bird/codebase_community/users/created_in_year", summary="Get users created in a specific year")
async def get_users_created_in_year(year: int = Query(..., description="Year of creation")):
    query = "SELECT DisplayName FROM users WHERE STRFTIME('%Y', CreationDate) = ?"
    cursor.execute(query, (year,))
    return cursor.fetchall()

# Endpoint to get count of users with last access date after a specific date
@app.get("/v1/bird/codebase_community/users/last_access_after", summary="Get count of users with last access date after a specific date")
async def get_users_last_access_after(date: str = Query(..., description="Date in YYYY-MM-DD format")):
    query = "SELECT COUNT(Id) FROM users WHERE date(LastAccessDate) > ?"
    cursor.execute(query, (date,))
    return cursor.fetchall()

# Endpoint to get user with maximum views
@app.get("/v1/bird/codebase_community/users/max_views", summary="Get user with maximum views")
async def get_user_max_views():
    query = "SELECT DisplayName FROM users WHERE Views = (SELECT MAX(Views) FROM users)"
    cursor.execute(query)
    return cursor.fetchall()

# Endpoint to get count of users with specific upvotes and downvotes
@app.get("/v1/bird/codebase_community/users/upvotes_downvotes", summary="Get count of users with specific upvotes and downvotes")
async def get_users_upvotes_downvotes(upvotes: int = Query(..., description="Minimum upvotes"), downvotes: int = Query(..., description="Minimum downvotes")):
    query = "SELECT COUNT(Id) FROM users WHERE Upvotes > ? AND Downvotes > ?"
    cursor.execute(query, (upvotes, downvotes))
    return cursor.fetchall()

# Endpoint to get count of users created after a specific year and with specific views
@app.get("/v1/bird/codebase_community/users/created_after_views", summary="Get count of users created after a specific year and with specific views")
async def get_users_created_after_views(year: int = Query(..., description="Year of creation"), views: int = Query(..., description="Minimum views")):
    query = "SELECT COUNT(id) FROM users WHERE STRFTIME('%Y', CreationDate) > ? AND Views > ?"
    cursor.execute(query, (year, views))
    return cursor.fetchall()

# Endpoint to get count of posts by a specific user
@app.get("/v1/bird/codebase_community/posts/count_by_user", summary="Get count of posts by a specific user")
async def get_posts_count_by_user(display_name: str = Query(..., description="Display name of the user")):
    query = "SELECT COUNT(T1.id) FROM posts AS T1 INNER JOIN users AS T2 ON T1.OwnerUserId = T2.Id WHERE T2.DisplayName = ?"
    cursor.execute(query, (display_name,))
    return cursor.fetchall()

# Endpoint to get post titles by a specific user
@app.get("/v1/bird/codebase_community/posts/titles_by_user", summary="Get post titles by a specific user")
async def get_post_titles_by_user(display_name: str = Query(..., description="Display name of the user")):
    query = "SELECT T1.Title FROM posts AS T1 INNER JOIN users AS T2 ON T1.OwnerUserId = T2.Id WHERE T2.DisplayName = ?"
    cursor.execute(query, (display_name,))
    return cursor.fetchall()

# Endpoint to get user display name for a specific post title
@app.get("/v1/bird/codebase_community/posts/user_by_title", summary="Get user display name for a specific post title")
async def get_user_by_post_title(title: str = Query(..., description="Title of the post")):
    query = "SELECT T2.DisplayName FROM posts AS T1 INNER JOIN users AS T2 ON T1.OwnerUserId = T2.Id WHERE T1.Title = ?"
    cursor.execute(query, (title,))
    return cursor.fetchall()

# Endpoint to get top post title by a specific user
@app.get("/v1/bird/codebase_community/posts/top_title_by_user", summary="Get top post title by a specific user")
async def get_top_post_title_by_user(display_name: str = Query(..., description="Display name of the user")):
    query = "SELECT T1.Title FROM posts AS T1 INNER JOIN users AS T2 ON T1.OwnerUserId = T2.Id WHERE T2.DisplayName = ? ORDER BY T1.ViewCount DESC LIMIT 1"
    cursor.execute(query, (display_name,))
    return cursor.fetchall()

# Endpoint to get user with the most favorite posts
@app.get("/v1/bird/codebase_community/posts/top_favorite_user", summary="Get user with the most favorite posts")
async def get_top_favorite_user():
    query = "SELECT T2.DisplayName FROM posts AS T1 INNER JOIN users AS T2 ON T1.OwnerUserId = T2.Id ORDER BY T1.FavoriteCount DESC LIMIT 1"
    cursor.execute(query)
    return cursor.fetchall()

# Endpoint to get sum of comments by a specific user
@app.get("/v1/bird/codebase_community/posts/sum_comments_by_user", summary="Get sum of comments by a specific user")
async def get_sum_comments_by_user(display_name: str = Query(..., description="Display name of the user")):
    query = "SELECT SUM(T1.CommentCount) FROM posts AS T1 INNER JOIN users AS T2 ON T1.OwnerUserId = T2.Id WHERE T2.DisplayName = ?"
    cursor.execute(query, (display_name,))
    return cursor.fetchall()

# Endpoint to get max answers by a specific user
@app.get("/v1/bird/codebase_community/posts/max_answers_by_user", summary="Get max answers by a specific user")
async def get_max_answers_by_user(display_name: str = Query(..., description="Display name of the user")):
    query = "SELECT MAX(T1.AnswerCount) FROM posts AS T1 INNER JOIN users AS T2 ON T1.OwnerUserId = T2.Id WHERE T2.DisplayName = ?"
    cursor.execute(query, (display_name,))
    return cursor.fetchall()

# Endpoint to get user who last edited a specific post
@app.get("/v1/bird/codebase_community/posts/last_editor_by_title", summary="Get user who last edited a specific post")
async def get_last_editor_by_post_title(title: str = Query(..., description="Title of the post")):
    query = "SELECT T2.DisplayName FROM posts AS T1 INNER JOIN users AS T2 ON T1.LastEditorUserId = T2.Id WHERE T1.Title = ?"
    cursor.execute(query, (title,))
    return cursor.fetchall()

# Endpoint to get count of posts by a specific user with no parent
@app.get("/v1/bird/codebase_community/posts/count_no_parent_by_user", summary="Get count of posts by a specific user with no parent")
async def get_count_no_parent_by_user(display_name: str = Query(..., description="Display name of the user")):
    query = "SELECT COUNT(T1.Id) FROM posts AS T1 INNER JOIN users AS T2 ON T1.OwnerUserId = T2.Id WHERE T2.DisplayName = ? AND T1.ParentId IS NULL"
    cursor.execute(query, (display_name,))
    return cursor.fetchall()

# Endpoint to get users with closed posts
@app.get("/v1/bird/codebase_community/posts/users_with_closed_posts", summary="Get users with closed posts")
async def get_users_with_closed_posts():
    query = "SELECT T2.DisplayName FROM posts AS T1 INNER JOIN users AS T2 ON T1.OwnerUserId = T2.Id WHERE T1.ClosedDate IS NOT NULL"
    cursor.execute(query)
    return cursor.fetchall()

# Endpoint to get count of posts with specific score and user age
@app.get("/v1/bird/codebase_community/posts/count_by_score_age", summary="Get count of posts with specific score and user age")
async def get_count_by_score_age(score: int = Query(..., description="Minimum score"), age: int = Query(..., description="Minimum age")):
    query = "SELECT COUNT(T1.Id) FROM posts AS T1 INNER JOIN users AS T2 ON T1.OwnerUserId = T2.Id WHERE T1.Score >= ? AND T2.Age > ?"
    cursor.execute(query, (score, age))
    return cursor.fetchall()

# Endpoint to get user location for a specific post title
@app.get("/v1/bird/codebase_community/posts/user_location_by_title", summary="Get user location for a specific post title")
async def get_user_location_by_post_title(title: str = Query(..., description="Title of the post")):
    query = "SELECT T2.Location FROM posts AS T1 INNER JOIN users AS T2 ON T1.OwnerUserId = T2.Id WHERE T1.Title = ?"
    cursor.execute(query, (title,))
    return cursor.fetchall()

# Endpoint to get post body by tag name
@app.get("/v1/bird/codebase_community/tags/post_body_by_tag", summary="Get post body by tag name")
async def get_post_body_by_tag(tag_name: str = Query(..., description="Name of the tag")):
    query = "SELECT T2.Body FROM tags AS T1 INNER JOIN posts AS T2 ON T2.Id = T1.ExcerptPostId WHERE T1.TagName = ?"
    cursor.execute(query, (tag_name,))
    return cursor.fetchall()

# Endpoint to get post body with the highest tag count
@app.get("/v1/bird/codebase_community/tags/top_post_body", summary="Get post body with the highest tag count")
async def get_top_post_body():
    query = "SELECT Body FROM posts WHERE id = (SELECT ExcerptPostId FROM tags ORDER BY Count DESC LIMIT 1)"
    cursor.execute(query)
    return cursor.fetchall()

# Endpoint to get count of badges for a given user
@app.get("/v1/bird/codebase_community/badges_count", summary="Get count of badges for a given user")
async def get_badges_count(display_name: str = Query(..., description="Display name of the user")):
    query = "SELECT COUNT(T1.Id) FROM badges AS T1 INNER JOIN users AS T2 ON T1.UserId = T2.Id WHERE T2.DisplayName = ?"
    cursor.execute(query, (display_name,))
    result = cursor.fetchone()
    return {"badges_count": result[0]}

# Endpoint to get badge names for a given user
@app.get("/v1/bird/codebase_community/badge_names", summary="Get badge names for a given user")
async def get_badge_names(display_name: str = Query(..., description="Display name of the user")):
    query = "SELECT T1.`Name` FROM badges AS T1 INNER JOIN users AS T2 ON T1.UserId = T2.Id WHERE T2.DisplayName = ?"
    cursor.execute(query, (display_name,))
    result = cursor.fetchall()
    return {"badge_names": [row[0] for row in result]}

# Endpoint to get count of badges for a given user in a specific year
@app.get("/v1/bird/codebase_community/badges_count_year", summary="Get count of badges for a given user in a specific year")
async def get_badges_count_year(year: int = Query(..., description="Year"), display_name: str = Query(..., description="Display name of the user")):
    query = "SELECT COUNT(T1.Id) FROM badges AS T1 INNER JOIN users AS T2 ON T1.UserId = T2.Id WHERE STRFTIME('%Y', T1.Date) = ? AND T2.DisplayName = ?"
    cursor.execute(query, (str(year), display_name))
    result = cursor.fetchone()
    return {"badges_count": result[0]}

# Endpoint to get user with the most badges
@app.get("/v1/bird/codebase_community/top_user_badges", summary="Get user with the most badges")
async def get_top_user_badges():
    query = "SELECT T2.DisplayName FROM badges AS T1 INNER JOIN users AS T2 ON T1.UserId = T2.Id GROUP BY T2.DisplayName ORDER BY COUNT(T1.Id) DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"top_user": result[0]}

# Endpoint to get average score of posts for a given user
@app.get("/v1/bird/codebase_community/average_post_score", summary="Get average score of posts for a given user")
async def get_average_post_score(display_name: str = Query(..., description="Display name of the user")):
    query = "SELECT AVG(T1.Score) FROM posts AS T1 INNER JOIN users AS T2 ON T1.OwnerUserId = T2.Id WHERE T2.DisplayName = ?"
    cursor.execute(query, (display_name,))
    result = cursor.fetchone()
    return {"average_score": result[0]}

# Endpoint to get ratio of badges to users with more than a certain number of views
@app.get("/v1/bird/codebase_community/badges_to_users_ratio", summary="Get ratio of badges to users with more than a certain number of views")
async def get_badges_to_users_ratio(views: int = Query(..., description="Number of views")):
    query = "SELECT CAST(COUNT(T1.Id) AS REAL) / COUNT(DISTINCT T2.DisplayName) FROM badges AS T1 INNER JOIN users AS T2 ON T1.UserId = T2.Id WHERE T2.Views > ?"
    cursor.execute(query, (views,))
    result = cursor.fetchone()
    return {"ratio": result[0]}

# Endpoint to get percentage of posts by users older than a certain age with a score greater than a certain value
@app.get("/v1/bird/codebase_community/posts_by_age_and_score", summary="Get percentage of posts by users older than a certain age with a score greater than a certain value")
async def get_posts_by_age_and_score(age: int = Query(..., description="Age"), score: int = Query(..., description="Score")):
    query = "SELECT CAST(SUM(IIF(T2.Age > ?, 1, 0)) AS REAL) * 100 / COUNT(T1.Id) FROM posts AS T1 INNER JOIN users AS T2 ON T1.OwnerUserId = T2.Id WHERE T1.Score > ?"
    cursor.execute(query, (age, score))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get count of votes for a given user on a specific date
@app.get("/v1/bird/codebase_community/votes_count", summary="Get count of votes for a given user on a specific date")
async def get_votes_count(user_id: int = Query(..., description="User ID"), creation_date: str = Query(..., description="Creation date")):
    query = "SELECT COUNT(Id) FROM votes WHERE UserId = ? AND CreationDate = ?"
    cursor.execute(query, (user_id, creation_date))
    result = cursor.fetchone()
    return {"votes_count": result[0]}

# Endpoint to get date with the most votes
@app.get("/v1/bird/codebase_community/top_votes_date", summary="Get date with the most votes")
async def get_top_votes_date():
    query = "SELECT CreationDate FROM votes GROUP BY CreationDate ORDER BY COUNT(Id) DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"top_votes_date": result[0]}

# Endpoint to get count of badges with a specific name
@app.get("/v1/bird/codebase_community/badges_count_by_name", summary="Get count of badges with a specific name")
async def get_badges_count_by_name(name: str = Query(..., description="Name of the badge")):
    query = "SELECT COUNT(Id) FROM badges WHERE Name = ?"
    cursor.execute(query, (name,))
    result = cursor.fetchone()
    return {"badges_count": result[0]}

# Endpoint to get title of the post with the highest-scoring comment
@app.get("/v1/bird/codebase_community/top_comment_post_title", summary="Get title of the post with the highest-scoring comment")
async def get_top_comment_post_title():
    query = "SELECT Title FROM posts WHERE Id = (SELECT PostId FROM comments ORDER BY Score DESC LIMIT 1)"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"post_title": result[0]}

# Endpoint to get count of posts with a specific view count
@app.get("/v1/bird/codebase_community/posts_count_by_view_count", summary="Get count of posts with a specific view count")
async def get_posts_count_by_view_count(view_count: int = Query(..., description="View count")):
    query = "SELECT COUNT(T1.Id) FROM posts AS T1 INNER JOIN comments AS T2 ON T1.Id = T2.PostId WHERE T1.ViewCount = ?"
    cursor.execute(query, (view_count,))
    result = cursor.fetchone()
    return {"posts_count": result[0]}

# Endpoint to get favorite count of a post with a specific comment date and user ID
@app.get("/v1/bird/codebase_community/post_favorite_count", summary="Get favorite count of a post with a specific comment date and user ID")
async def get_post_favorite_count(creation_date: str = Query(..., description="Creation date"), user_id: int = Query(..., description="User ID")):
    query = "SELECT T1.FavoriteCount FROM posts AS T1 INNER JOIN comments AS T2 ON T1.Id = T2.PostId WHERE T2.CreationDate = ? AND T2.UserId = ?"
    cursor.execute(query, (creation_date, user_id))
    result = cursor.fetchone()
    return {"favorite_count": result[0]}

# Endpoint to get text of comments for a post with a specific parent ID and comment count
@app.get("/v1/bird/codebase_community/post_comments_text", summary="Get text of comments for a post with a specific parent ID and comment count")
async def get_post_comments_text(parent_id: int = Query(..., description="Parent ID"), comment_count: int = Query(..., description="Comment count")):
    query = "SELECT T2.Text FROM posts AS T1 INNER JOIN comments AS T2 ON T1.Id = T2.PostId WHERE T1.ParentId = ? AND T1.CommentCount = ?"
    cursor.execute(query, (parent_id, comment_count))
    result = cursor.fetchall()
    return {"comments_text": [row[0] for row in result]}

# Endpoint to get whether a post is well-finished based on user ID and creation date
@app.get("/v1/bird/codebase_community/post_well_finished", summary="Get whether a post is well-finished based on user ID and creation date")
async def get_post_well_finished(user_id: int = Query(..., description="User ID"), creation_date: str = Query(..., description="Creation date")):
    query = "SELECT IIF(T2.ClosedDate IS NULL, 'NOT well-finished', 'well-finished') AS result FROM comments AS T1 INNER JOIN posts AS T2 ON T1.PostId = T2.Id WHERE T1.UserId = ? AND T1.CreationDate = ?"
    cursor.execute(query, (user_id, creation_date))
    result = cursor.fetchone()
    return {"result": result[0]}

# Endpoint to get reputation of a user for a specific post
@app.get("/v1/bird/codebase_community/user_reputation", summary="Get reputation of a user for a specific post")
async def get_user_reputation(post_id: int = Query(..., description="Post ID")):
    query = "SELECT T1.Reputation FROM users AS T1 INNER JOIN posts AS T2 ON T1.Id = T2.OwnerUserId WHERE T2.Id = ?"
    cursor.execute(query, (post_id,))
    result = cursor.fetchone()
    return {"reputation": result[0]}

# Endpoint to get count of posts for a given user
@app.get("/v1/bird/codebase_community/posts_count_by_user", summary="Get count of posts for a given user")
async def get_posts_count_by_user(display_name: str = Query(..., description="Display name of the user")):
    query = "SELECT COUNT(T1.Id) FROM users AS T1 INNER JOIN posts AS T2 ON T1.Id = T2.OwnerUserId WHERE T1.DisplayName = ?"
    cursor.execute(query, (display_name,))
    result = cursor.fetchone()
    return {"posts_count": result[0]}

# Endpoint to get display name of a user for a specific vote
@app.get("/v1/bird/codebase_community/user_display_name_by_vote", summary="Get display name of a user for a specific vote")
async def get_user_display_name_by_vote(vote_id: int = Query(..., description="Vote ID")):
    query = "SELECT T1.DisplayName FROM users AS T1 INNER JOIN votes AS T2 ON T1.Id = T2.UserId WHERE T2.Id = ?"
    cursor.execute(query, (vote_id,))
    result = cursor.fetchone()
    return {"display_name": result[0]}

# Endpoint to get count of posts with a specific title
@app.get("/v1/bird/codebase_community/posts_count_by_title", summary="Get count of posts with a specific title")
async def get_posts_count_by_title(title: str = Query(..., description="Title of the post")):
    query = "SELECT COUNT(T1.Id) FROM posts AS T1 INNER JOIN votes AS T2 ON T1.Id = T2.PostId WHERE T1.Title LIKE ?"
    cursor.execute(query, (f"%{title}%",))
    result = cursor.fetchone()
    return {"posts_count": result[0]}

# Endpoint to get badge names for a given user
@app.get("/v1/bird/codebase_community/badge_names_by_user", summary="Get badge names for a given user")
async def get_badge_names_by_user(display_name: str = Query(..., description="Display name of the user")):
    query = "SELECT T2.Name FROM users AS T1 INNER JOIN badges AS T2 ON T1.Id = T2.UserId WHERE T1.DisplayName = ?"
    cursor.execute(query, (display_name,))
    result = cursor.fetchall()
    return {"badge_names": [row[0] for row in result]}
# Endpoint to get the ratio of votes to posts for a given user
@app.get("/v1/bird/codebase_community/votes_to_posts_ratio", summary="Get the ratio of votes to posts for a given user")
async def get_votes_to_posts_ratio(user_id: int = Query(..., description="ID of the user")):
    query = """
    SELECT CAST(COUNT(T2.Id) AS REAL) / COUNT(DISTINCT T1.Id)
    FROM votes AS T1
    INNER JOIN posts AS T2 ON T1.UserId = T2.OwnerUserId
    WHERE T1.UserId = ?
    """
    cursor.execute(query, (user_id,))
    result = cursor.fetchone()
    return {"ratio": result[0]}

# Endpoint to get the view count for a given post title
@app.get("/v1/bird/codebase_community/post_view_count", summary="Get the view count for a given post title")
async def get_post_view_count(title: str = Query(..., description="Title of the post")):
    query = """
    SELECT ViewCount FROM posts WHERE Title = ?
    """
    cursor.execute(query, (title,))
    result = cursor.fetchone()
    return {"view_count": result[0]}

# Endpoint to get the text of comments with a given score
@app.get("/v1/bird/codebase_community/comment_text", summary="Get the text of comments with a given score")
async def get_comment_text(score: int = Query(..., description="Score of the comment")):
    query = """
    SELECT Text FROM comments WHERE Score = ?
    """
    cursor.execute(query, (score,))
    result = cursor.fetchall()
    return {"comments": [row[0] for row in result]}

# Endpoint to get the display name of users with a given website URL
@app.get("/v1/bird/codebase_community/user_display_name", summary="Get the display name of users with a given website URL")
async def get_user_display_name(website_url: str = Query(..., description="Website URL of the user")):
    query = """
    SELECT DisplayName FROM users WHERE WebsiteUrl = ?
    """
    cursor.execute(query, (website_url,))
    result = cursor.fetchall()
    return {"display_names": [row[0] for row in result]}

# Endpoint to get the badge names for a given user display name
@app.get("/v1/bird/codebase_community/user_badges", summary="Get the badge names for a given user display name")
async def get_user_badges(display_name: str = Query(..., description="Display name of the user")):
    query = """
    SELECT T2.Name FROM users AS T1
    INNER JOIN badges AS T2 ON T1.Id = T2.UserId
    WHERE T1.DisplayName = ?
    """
    cursor.execute(query, (display_name,))
    result = cursor.fetchall()
    return {"badges": [row[0] for row in result]}

# Endpoint to get the display name of users who made a specific comment
@app.get("/v1/bird/codebase_community/user_by_comment", summary="Get the display name of users who made a specific comment")
async def get_user_by_comment(comment_text: str = Query(..., description="Text of the comment")):
    query = """
    SELECT T1.DisplayName FROM users AS T1
    INNER JOIN comments AS T2 ON T1.Id = T2.UserId
    WHERE T2.Text = ?
    """
    cursor.execute(query, (comment_text,))
    result = cursor.fetchall()
    return {"display_names": [row[0] for row in result]}

# Endpoint to get the comments made by a specific user
@app.get("/v1/bird/codebase_community/comments_by_user", summary="Get the comments made by a specific user")
async def get_comments_by_user(display_name: str = Query(..., description="Display name of the user")):
    query = """
    SELECT T2.Text FROM users AS T1
    INNER JOIN comments AS T2 ON T1.Id = T2.UserId
    WHERE T1.DisplayName = ?
    """
    cursor.execute(query, (display_name,))
    result = cursor.fetchall()
    return {"comments": [row[0] for row in result]}

# Endpoint to get the display name and reputation of users who posted a specific post
@app.get("/v1/bird/codebase_community/user_by_post", summary="Get the display name and reputation of users who posted a specific post")
async def get_user_by_post(post_title: str = Query(..., description="Title of the post")):
    query = """
    SELECT T1.DisplayName, T1.Reputation FROM users AS T1
    INNER JOIN posts AS T2 ON T1.Id = T2.OwnerUserId
    WHERE T2.Title = ?
    """
    cursor.execute(query, (post_title,))
    result = cursor.fetchall()
    return {"users": [{"display_name": row[0], "reputation": row[1]} for row in result]}

# Endpoint to get the comments for a specific post
@app.get("/v1/bird/codebase_community/comments_by_post", summary="Get the comments for a specific post")
async def get_comments_by_post(post_title: str = Query(..., description="Title of the post")):
    query = """
    SELECT T1.Text FROM comments AS T1
    INNER JOIN posts AS T2 ON T1.PostId = T2.Id
    WHERE T2.Title = ?
    """
    cursor.execute(query, (post_title,))
    result = cursor.fetchall()
    return {"comments": [row[0] for row in result]}

# Endpoint to get the display names of users with a specific badge
@app.get("/v1/bird/codebase_community/users_by_badge", summary="Get the display names of users with a specific badge")
async def get_users_by_badge(badge_name: str = Query(..., description="Name of the badge")):
    query = """
    SELECT T1.DisplayName FROM users AS T1
    INNER JOIN badges AS T2 ON T1.Id = T2.UserId
    WHERE T2.Name = ?
    LIMIT 10
    """
    cursor.execute(query, (badge_name,))
    result = cursor.fetchall()
    return {"display_names": [row[0] for row in result]}

# Endpoint to get the display name of the user who posted a specific post
@app.get("/v1/bird/codebase_community/user_by_post_title", summary="Get the display name of the user who posted a specific post")
async def get_user_by_post_title(post_title: str = Query(..., description="Title of the post")):
    query = """
    SELECT T2.DisplayName FROM posts AS T1
    INNER JOIN users AS T2 ON T1.OwnerUserId = T2.Id
    WHERE T1.Title = ?
    """
    cursor.execute(query, (post_title,))
    result = cursor.fetchone()
    return {"display_name": result[0]}

# Endpoint to get the post titles for a specific user
@app.get("/v1/bird/codebase_community/posts_by_user", summary="Get the post titles for a specific user")
async def get_posts_by_user(display_name: str = Query(..., description="Display name of the user")):
    query = """
    SELECT T1.Title FROM posts AS T1
    INNER JOIN users AS T2 ON T1.OwnerUserId = T2.Id
    WHERE T2.DisplayName = ?
    """
    cursor.execute(query, (display_name,))
    result = cursor.fetchall()
    return {"post_titles": [row[0] for row in result]}

# Endpoint to get the sum of scores and website URL for a specific user
@app.get("/v1/bird/codebase_community/user_score_sum", summary="Get the sum of scores and website URL for a specific user")
async def get_user_score_sum(display_name: str = Query(..., description="Display name of the user")):
    query = """
    SELECT SUM(T1.Score), T2.WebsiteUrl FROM posts AS T1
    INNER JOIN users AS T2 ON T1.LastEditorUserId = T2.Id
    WHERE T2.DisplayName = ?
    GROUP BY T2.WebsiteUrl
    """
    cursor.execute(query, (display_name,))
    result = cursor.fetchall()
    return {"score_sums": [{"sum": row[0], "website_url": row[1]} for row in result]}

# Endpoint to get the comments for a specific post
@app.get("/v1/bird/codebase_community/post_comments", summary="Get the comments for a specific post")
async def get_post_comments(post_title: str = Query(..., description="Title of the post")):
    query = """
    SELECT T2.Comment FROM posts AS T1
    INNER JOIN postHistory AS T2 ON T1.Id = T2.PostId
    WHERE T1.Title = ?
    """
    cursor.execute(query, (post_title,))
    result = cursor.fetchall()
    return {"comments": [row[0] for row in result]}

# Endpoint to get the sum of bounty amounts for posts with a specific title
@app.get("/v1/bird/codebase_community/bounty_sum", summary="Get the sum of bounty amounts for posts with a specific title")
async def get_bounty_sum(post_title: str = Query(..., description="Title of the post")):
    query = """
    SELECT SUM(T2.BountyAmount) FROM posts AS T1
    INNER JOIN votes AS T2 ON T1.Id = T2.PostId
    WHERE T1.Title LIKE ?
    """
    cursor.execute(query, (f"%{post_title}%",))
    result = cursor.fetchone()
    return {"bounty_sum": result[0]}

# Endpoint to get the display names and post titles for a specific bounty amount and title
@app.get("/v1/bird/codebase_community/users_by_bounty", summary="Get the display names and post titles for a specific bounty amount and title")
async def get_users_by_bounty(bounty_amount: int = Query(..., description="Bounty amount"), post_title: str = Query(..., description="Title of the post")):
    query = """
    SELECT T3.DisplayName, T1.Title FROM posts AS T1
    INNER JOIN votes AS T2 ON T1.Id = T2.PostId
    INNER JOIN users AS T3 ON T3.Id = T2.UserId
    WHERE T2.BountyAmount = ? AND T1.Title LIKE ?
    """
    cursor.execute(query, (bounty_amount, f"%{post_title}%"))
    result = cursor.fetchall()
    return {"users": [{"display_name": row[0], "post_title": row[1]} for row in result]}

# Endpoint to get the average view count, title, and text for comments with a specific tag
@app.get("/v1/bird/codebase_community/avg_view_count", summary="Get the average view count, title, and text for comments with a specific tag")
async def get_avg_view_count(tag: str = Query(..., description="Tag of the comments")):
    query = """
    SELECT AVG(T2.ViewCount), T2.Title, T1.Text FROM comments AS T1
    INNER JOIN posts AS T2 ON T2.Id = T1.PostId
    WHERE T2.Tags = ?
    GROUP BY T2.Title, T1.Text
    """
    cursor.execute(query, (tag,))
    result = cursor.fetchall()
    return {"avg_view_counts": [{"avg_view_count": row[0], "title": row[1], "text": row[2]} for row in result]}

# Endpoint to get the count of comments for a specific user
@app.get("/v1/bird/codebase_community/comment_count", summary="Get the count of comments for a specific user")
async def get_comment_count(user_id: int = Query(..., description="ID of the user")):
    query = """
    SELECT COUNT(Id) FROM comments WHERE UserId = ?
    """
    cursor.execute(query, (user_id,))
    result = cursor.fetchone()
    return {"comment_count": result[0]}

# Endpoint to get the user ID with the maximum reputation
@app.get("/v1/bird/codebase_community/max_reputation_user", summary="Get the user ID with the maximum reputation")
async def get_max_reputation_user():
    query = """
    SELECT Id FROM users WHERE Reputation = (SELECT MAX(Reputation) FROM users)
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"user_id": result[0]}

# Endpoint to get the user ID with the minimum views
@app.get("/v1/bird/codebase_community/min_views_user", summary="Get the user ID with the minimum views")
async def get_min_views_user():
    query = """
    SELECT Id FROM users WHERE Views = (SELECT MIN(Views) FROM users)
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"user_id": result[0]}
# Endpoint to get count of badges for a given year and name
@app.get("/v1/bird/codebase_community/badges_count", summary="Get count of badges for a given year and name")
async def get_badges_count(year: str = Query(..., description="Year to filter badges"), name: str = Query(..., description="Name of the badge")):
    query = f"SELECT COUNT(Id) FROM badges WHERE STRFTIME('%Y', Date) = ? AND Name = ?"
    cursor.execute(query, (year, name))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get count of users with more than a certain number of badges
@app.get("/v1/bird/codebase_community/users_with_badges_count", summary="Get count of users with more than a certain number of badges")
async def get_users_with_badges_count(num: int = Query(..., description="Minimum number of badges")):
    query = f"SELECT COUNT(UserId) FROM ( SELECT UserId, COUNT(Name) AS num FROM badges GROUP BY UserId ) T WHERE T.num > ?"
    cursor.execute(query, (num,))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get count of distinct badges for a given location and badge names
@app.get("/v1/bird/codebase_community/distinct_badges_count", summary="Get count of distinct badges for a given location and badge names")
async def get_distinct_badges_count(location: str = Query(..., description="Location to filter users"), names: str = Query(..., description="Comma-separated list of badge names")):
    names_list = names.split(',')
    query = f"SELECT COUNT(DISTINCT T1.Id) FROM badges AS T1 INNER JOIN users AS T2 ON T1.UserId = T2.Id WHERE T1.Name IN ({','.join(['?']*len(names_list))}) AND T2.Location = ?"
    cursor.execute(query, (*names_list, location))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get user details for a given post ID
@app.get("/v1/bird/codebase_community/user_details_by_post", summary="Get user details for a given post ID")
async def get_user_details_by_post(post_id: int = Query(..., description="ID of the post")):
    query = f"SELECT T2.Id, T2.Reputation FROM comments AS T1 INNER JOIN users AS T2 ON T1.UserId = T2.Id WHERE T1.PostId = ?"
    cursor.execute(query, (post_id,))
    result = cursor.fetchall()
    return {"user_details": result}

# Endpoint to get user IDs with a certain view count and post history type count
@app.get("/v1/bird/codebase_community/user_ids_by_view_count", summary="Get user IDs with a certain view count and post history type count")
async def get_user_ids_by_view_count(view_count: int = Query(..., description="Minimum view count"), post_history_type_count: int = Query(..., description="Number of distinct post history types")):
    query = f"SELECT T2.UserId FROM users AS T1 INNER JOIN postHistory AS T2 ON T1.Id = T2.UserId INNER JOIN posts AS T3 ON T2.PostId = T3.Id WHERE T3.ViewCount >= ? GROUP BY T2.UserId HAVING COUNT(DISTINCT T2.PostHistoryTypeId) = ?"
    cursor.execute(query, (view_count, post_history_type_count))
    result = cursor.fetchall()
    return {"user_ids": result}

# Endpoint to get the most common badge name for a user
@app.get("/v1/bird/codebase_community/most_common_badge", summary="Get the most common badge name for a user")
async def get_most_common_badge():
    query = f"SELECT Name FROM badges AS T1 INNER JOIN comments AS T2 ON T1.UserId = t2.UserId GROUP BY T2.UserId ORDER BY COUNT(T2.UserId) DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"badge_name": result[0]}

# Endpoint to get count of badges for a given location and badge name
@app.get("/v1/bird/codebase_community/badges_count_by_location", summary="Get count of badges for a given location and badge name")
async def get_badges_count_by_location(location: str = Query(..., description="Location to filter users"), name: str = Query(..., description="Name of the badge")):
    query = f"SELECT COUNT(T1.Id) FROM badges AS T1 INNER JOIN users AS T2 ON T1.UserId = T2.Id WHERE T2.Location = ? AND T1.Name = ?"
    cursor.execute(query, (location, name))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get the percentage change in badges count between two years for a given badge name
@app.get("/v1/bird/codebase_community/badges_percentage_change", summary="Get the percentage change in badges count between two years for a given badge name")
async def get_badges_percentage_change(name: str = Query(..., description="Name of the badge")):
    query = f"SELECT CAST(SUM(IIF(STRFTIME('%Y', Date) = '2010', 1, 0)) AS REAL) * 100 / COUNT(Id) - CAST(SUM(IIF(STRFTIME('%Y', Date) = '2011', 1, 0)) AS REAL) * 100 / COUNT(Id) FROM badges WHERE Name = ?"
    cursor.execute(query, (name,))
    result = cursor.fetchone()
    return {"percentage_change": result[0]}

# Endpoint to get post history type ID and number of users for a given post ID
@app.get("/v1/bird/codebase_community/post_history_type_users", summary="Get post history type ID and number of users for a given post ID")
async def get_post_history_type_users(post_id: int = Query(..., description="ID of the post")):
    query = f"SELECT T1.PostHistoryTypeId, (SELECT COUNT(DISTINCT UserId) FROM comments WHERE PostId = ?) AS NumberOfUsers FROM postHistory AS T1 WHERE T1.PostId = ?"
    cursor.execute(query, (post_id, post_id))
    result = cursor.fetchall()
    return {"post_history_type_users": result}

# Endpoint to get view count for a given post ID
@app.get("/v1/bird/codebase_community/view_count_by_post", summary="Get view count for a given post ID")
async def get_view_count_by_post(post_id: int = Query(..., description="ID of the post")):
    query = f"SELECT T1.ViewCount FROM posts AS T1 INNER JOIN postLinks AS T2 ON T1.Id = T2.PostId WHERE T2.PostId = ?"
    cursor.execute(query, (post_id,))
    result = cursor.fetchone()
    return {"view_count": result[0]}

# Endpoint to get score and link type ID for a given post ID
@app.get("/v1/bird/codebase_community/score_link_type_by_post", summary="Get score and link type ID for a given post ID")
async def get_score_link_type_by_post(post_id: int = Query(..., description="ID of the post")):
    query = f"SELECT T1.Score, T2.LinkTypeId FROM posts AS T1 INNER JOIN postLinks AS T2 ON T1.Id = T2.PostId WHERE T2.PostId = ?"
    cursor.execute(query, (post_id,))
    result = cursor.fetchone()
    return {"score": result[0], "link_type_id": result[1]}

# Endpoint to get post history for posts with a score greater than a given value
@app.get("/v1/bird/codebase_community/post_history_by_score", summary="Get post history for posts with a score greater than a given value")
async def get_post_history_by_score(score: int = Query(..., description="Minimum score of the post")):
    query = f"SELECT PostId, UserId FROM postHistory WHERE PostId IN ( SELECT Id FROM posts WHERE Score > ? )"
    cursor.execute(query, (score,))
    result = cursor.fetchall()
    return {"post_history": result}

# Endpoint to get sum of favorite counts for a given user ID and year
@app.get("/v1/bird/codebase_community/favorite_count_sum", summary="Get sum of favorite counts for a given user ID and year")
async def get_favorite_count_sum(user_id: int = Query(..., description="ID of the user"), year: str = Query(..., description="Year to filter posts")):
    query = f"SELECT SUM(DISTINCT FavoriteCount) FROM posts WHERE Id IN ( SELECT PostId FROM postHistory WHERE UserId = ? AND STRFTIME('%Y', CreationDate) = ? )"
    cursor.execute(query, (user_id, year))
    result = cursor.fetchone()
    return {"favorite_count_sum": result[0]}

# Endpoint to get average upvotes and age for users with more than a certain number of posts
@app.get("/v1/bird/codebase_community/avg_upvotes_age", summary="Get average upvotes and age for users with more than a certain number of posts")
async def get_avg_upvotes_age(post_count: int = Query(..., description="Minimum number of posts")):
    query = f"SELECT AVG(T1.UpVotes), AVG(T1.Age) FROM users AS T1 INNER JOIN ( SELECT OwnerUserId, COUNT(*) AS post_count FROM posts GROUP BY OwnerUserId HAVING post_count > ?) AS T2 ON T1.Id = T2.OwnerUserId"
    cursor.execute(query, (post_count,))
    result = cursor.fetchone()
    return {"avg_upvotes": result[0], "avg_age": result[1]}

# Endpoint to get count of badges for a given badge name
@app.get("/v1/bird/codebase_community/badges_count_by_name", summary="Get count of badges for a given badge name")
async def get_badges_count_by_name(name: str = Query(..., description="Name of the badge")):
    query = f"SELECT COUNT(id) FROM badges WHERE Name = ?"
    cursor.execute(query, (name,))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get badge names for a given date
@app.get("/v1/bird/codebase_community/badges_by_date", summary="Get badge names for a given date")
async def get_badges_by_date(date: str = Query(..., description="Date to filter badges")):
    query = f"SELECT Name FROM badges WHERE Date = ?"
    cursor.execute(query, (date,))
    result = cursor.fetchall()
    return {"badge_names": result}

# Endpoint to get count of comments with a score greater than a given value
@app.get("/v1/bird/codebase_community/comments_count_by_score", summary="Get count of comments with a score greater than a given value")
async def get_comments_count_by_score(score: int = Query(..., description="Minimum score of the comment")):
    query = f"SELECT COUNT(id) FROM comments WHERE score > ?"
    cursor.execute(query, (score,))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get comment text for a given creation date
@app.get("/v1/bird/codebase_community/comment_text_by_date", summary="Get comment text for a given creation date")
async def get_comment_text_by_date(date: str = Query(..., description="Creation date of the comment")):
    query = f"SELECT Text FROM comments WHERE CreationDate = ?"
    cursor.execute(query, (date,))
    result = cursor.fetchall()
    return {"comment_text": result}

# Endpoint to get count of posts with a given score
@app.get("/v1/bird/codebase_community/posts_count_by_score", summary="Get count of posts with a given score")
async def get_posts_count_by_score(score: int = Query(..., description="Score of the post")):
    query = f"SELECT COUNT(id) FROM posts WHERE Score = ?"
    cursor.execute(query, (score,))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get the most reputable user's badge name
@app.get("/v1/bird/codebase_community/most_reputable_user_badge", summary="Get the most reputable user's badge name")
async def get_most_reputable_user_badge():
    query = f"SELECT T2.name FROM users AS T1 INNER JOIN badges AS T2 ON T1.Id = T2.UserId ORDER BY T1.Reputation DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"badge_name": result[0]}
# Endpoint to get reputation for a given date
@app.get("/v1/bird/codebase_community/reputation", summary="Get reputation for a given date")
async def get_reputation_for_date(date: str = Query(..., description="Date in 'YYYY-MM-DD HH:MM:SS.SSS' format")):
    query = "SELECT T1.Reputation FROM users AS T1 INNER JOIN badges AS T2 ON T1.Id = T2.UserId WHERE T2.Date = ?"
    cursor.execute(query, (date,))
    result = cursor.fetchall()
    return result

# Endpoint to get badge name for a given display name
@app.get("/v1/bird/codebase_community/badge_name", summary="Get badge name for a given display name")
async def get_badge_name_for_display_name(display_name: str = Query(..., description="Display name of the user")):
    query = "SELECT T2.Name FROM users AS T1 INNER JOIN badges AS T2 ON T1.Id = T2.UserId WHERE T1.DisplayName = ?"
    cursor.execute(query, (display_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get badge date for a given location
@app.get("/v1/bird/codebase_community/badge_date", summary="Get badge date for a given location")
async def get_badge_date_for_location(location: str = Query(..., description="Location of the user")):
    query = "SELECT T2.Date FROM users AS T1 INNER JOIN badges AS T2 ON T1.Id = T2.UserId WHERE T1.Location = ?"
    cursor.execute(query, (location,))
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of users with a specific badge name
@app.get("/v1/bird/codebase_community/badge_percentage", summary="Get percentage of users with a specific badge name")
async def get_badge_percentage(badge_name: str = Query(..., description="Name of the badge")):
    query = """
    SELECT CAST(COUNT(T1.Id) AS REAL) * 100 / (SELECT COUNT(Id) FROM users)
    FROM users AS T1 INNER JOIN badges AS T2 ON T1.Id = T2.UserId
    WHERE T2.Name = ?
    """
    cursor.execute(query, (badge_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of users with a specific badge name and age range
@app.get("/v1/bird/codebase_community/badge_age_percentage", summary="Get percentage of users with a specific badge name and age range")
async def get_badge_age_percentage(badge_name: str = Query(..., description="Name of the badge"), min_age: int = Query(..., description="Minimum age"), max_age: int = Query(..., description="Maximum age")):
    query = """
    SELECT CAST(SUM(IIF(T2.Age BETWEEN ? AND ?, 1, 0)) AS REAL) * 100 / COUNT(T1.Id)
    FROM badges AS T1 INNER JOIN users AS T2 ON T1.UserId = T2.Id
    WHERE T1.Name = ?
    """
    cursor.execute(query, (min_age, max_age, badge_name))
    result = cursor.fetchall()
    return result

# Endpoint to get comment score for a given creation date
@app.get("/v1/bird/codebase_community/comment_score", summary="Get comment score for a given creation date")
async def get_comment_score_for_creation_date(creation_date: str = Query(..., description="Creation date in 'YYYY-MM-DD HH:MM:SS.SSS' format")):
    query = "SELECT T1.Score FROM comments AS T1 INNER JOIN posts AS T2 ON T1.PostId = T2.Id WHERE T1.CreationDate = ?"
    cursor.execute(query, (creation_date,))
    result = cursor.fetchall()
    return result

# Endpoint to get comment text for a given creation date
@app.get("/v1/bird/codebase_community/comment_text", summary="Get comment text for a given creation date")
async def get_comment_text_for_creation_date(creation_date: str = Query(..., description="Creation date in 'YYYY-MM-DD HH:MM:SS.SSS' format")):
    query = "SELECT T1.Text FROM comments AS T1 INNER JOIN posts AS T2 ON T1.PostId = T2.Id WHERE T1.CreationDate = ?"
    cursor.execute(query, (creation_date,))
    result = cursor.fetchall()
    return result

# Endpoint to get user age for a given location
@app.get("/v1/bird/codebase_community/user_age", summary="Get user age for a given location")
async def get_user_age_for_location(location: str = Query(..., description="Location of the user")):
    query = "SELECT T1.Age FROM users AS T1 INNER JOIN badges AS T2 ON T1.Id = T2.UserId WHERE T1.Location = ?"
    cursor.execute(query, (location,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of users with a specific badge name and age range
@app.get("/v1/bird/codebase_community/user_badge_age_count", summary="Get count of users with a specific badge name and age range")
async def get_user_badge_age_count(badge_name: str = Query(..., description="Name of the badge"), min_age: int = Query(..., description="Minimum age"), max_age: int = Query(..., description="Maximum age")):
    query = "SELECT COUNT(T1.Id) FROM users AS T1 INNER JOIN badges AS T2 ON T1.Id = T2.UserId WHERE T2.Name = ? AND T1.Age BETWEEN ? AND ?"
    cursor.execute(query, (badge_name, min_age, max_age))
    result = cursor.fetchall()
    return result

# Endpoint to get user views for a given date
@app.get("/v1/bird/codebase_community/user_views", summary="Get user views for a given date")
async def get_user_views_for_date(date: str = Query(..., description="Date in 'YYYY-MM-DD HH:MM:SS.SSS' format")):
    query = "SELECT T1.Views FROM users AS T1 INNER JOIN badges AS T2 ON T1.Id = T2.UserId WHERE T2.Date = ?"
    cursor.execute(query, (date,))
    result = cursor.fetchall()
    return result

# Endpoint to get badge name for a user with minimum reputation
@app.get("/v1/bird/codebase_community/badge_name_min_reputation", summary="Get badge name for a user with minimum reputation")
async def get_badge_name_min_reputation():
    query = "SELECT T2.Name FROM users AS T1 INNER JOIN badges AS T2 ON T1.Id = T2.UserId WHERE T1.Reputation = (SELECT MIN(Reputation) FROM users)"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get badge name for a given display name
@app.get("/v1/bird/codebase_community/badge_name_display_name", summary="Get badge name for a given display name")
async def get_badge_name_display_name(display_name: str = Query(..., description="Display name of the user")):
    query = "SELECT T2.Name FROM users AS T1 INNER JOIN badges AS T2 ON T1.Id = T2.UserId WHERE T1.DisplayName = ?"
    cursor.execute(query, (display_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of users with a specific badge name and age greater than a value
@app.get("/v1/bird/codebase_community/user_badge_age_count_greater", summary="Get count of users with a specific badge name and age greater than a value")
async def get_user_badge_age_count_greater(badge_name: str = Query(..., description="Name of the badge"), age: int = Query(..., description="Age of the user")):
    query = "SELECT COUNT(T1.Id) FROM users AS T1 INNER JOIN badges AS T2 ON T1.Id = T2.UserId WHERE T1.Age > ? AND T2.Name = ?"
    cursor.execute(query, (age, badge_name))
    result = cursor.fetchall()
    return result

# Endpoint to get display name for a given user id
@app.get("/v1/bird/codebase_community/user_display_name", summary="Get display name for a given user id")
async def get_user_display_name(user_id: int = Query(..., description="ID of the user")):
    query = "SELECT DisplayName FROM users WHERE Id = ?"
    cursor.execute(query, (user_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of users for a given location
@app.get("/v1/bird/codebase_community/user_count_location", summary="Get count of users for a given location")
async def get_user_count_location(location: str = Query(..., description="Location of the user")):
    query = "SELECT COUNT(Id) FROM users WHERE Location = ?"
    cursor.execute(query, (location,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of votes for a given year
@app.get("/v1/bird/codebase_community/vote_count_year", summary="Get count of votes for a given year")
async def get_vote_count_year(year: int = Query(..., description="Year in 'YYYY' format")):
    query = "SELECT COUNT(id) FROM votes WHERE STRFTIME('%Y', CreationDate) = ?"
    cursor.execute(query, (str(year),))
    result = cursor.fetchall()
    return result

# Endpoint to get count of users for a given age range
@app.get("/v1/bird/codebase_community/user_count_age_range", summary="Get count of users for a given age range")
async def get_user_count_age_range(min_age: int = Query(..., description="Minimum age"), max_age: int = Query(..., description="Maximum age")):
    query = "SELECT COUNT(id) FROM users WHERE Age BETWEEN ? AND ?"
    cursor.execute(query, (min_age, max_age))
    result = cursor.fetchall()
    return result

# Endpoint to get user details with maximum views
@app.get("/v1/bird/codebase_community/user_max_views", summary="Get user details with maximum views")
async def get_user_max_views():
    query = "SELECT Id, DisplayName FROM users WHERE Views = (SELECT MAX(Views) FROM users)"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get vote ratio for two given years
@app.get("/v1/bird/codebase_community/vote_ratio", summary="Get vote ratio for two given years")
async def get_vote_ratio(year1: int = Query(..., description="First year in 'YYYY' format"), year2: int = Query(..., description="Second year in 'YYYY' format")):
    query = """
    SELECT CAST(SUM(IIF(STRFTIME('%Y', CreationDate) = ?, 1, 0)) AS REAL) / SUM(IIF(STRFTIME('%Y', CreationDate) = ?, 1, 0))
    FROM votes
    """
    cursor.execute(query, (str(year1), str(year2)))
    result = cursor.fetchall()
    return result

# Endpoint to get tags for a given display name
@app.get("/v1/bird/codebase_community/user_tags", summary="Get tags for a given display name")
async def get_user_tags(display_name: str = Query(..., description="Display name of the user")):
    query = """
    SELECT T3.Tags FROM users AS T1
    INNER JOIN postHistory AS T2 ON T1.Id = T2.UserId
    INNER JOIN posts AS T3 ON T2.PostId = T3.Id
    WHERE T1.DisplayName = ?
    """
    cursor.execute(query, (display_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of users with a specific display name
@app.get("/v1/bird/codebase_community/user_post_count", summary="Get count of users with a specific display name")
async def get_user_post_count(display_name: str = Query(..., description="Display name of the user")):
    query = f"SELECT COUNT(T1.Id) FROM users AS T1 INNER JOIN postHistory AS T2 ON T1.Id = T2.UserId WHERE T1.DisplayName = ?"
    cursor.execute(query, (display_name,))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get count of users with a specific display name and votes
@app.get("/v1/bird/codebase_community/user_post_vote_count", summary="Get count of users with a specific display name and votes")
async def get_user_post_vote_count(display_name: str = Query(..., description="Display name of the user")):
    query = f"SELECT COUNT(T1.Id) FROM users AS T1 INNER JOIN postHistory AS T2 ON T1.Id = T2.UserId INNER JOIN votes AS T3 ON T3.PostId = T2.PostId WHERE T1.DisplayName = ?"
    cursor.execute(query, (display_name,))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get post ID for a specific user ordered by answer count
@app.get("/v1/bird/codebase_community/user_post_id", summary="Get post ID for a specific user ordered by answer count")
async def get_user_post_id(display_name: str = Query(..., description="Display name of the user")):
    query = f"SELECT T2.PostId FROM users AS T1 INNER JOIN postHistory AS T2 ON T1.Id = T2.UserId INNER JOIN posts AS T3 ON T2.PostId = T3.Id WHERE T1.DisplayName = ? ORDER BY T3.AnswerCount DESC LIMIT 1"
    cursor.execute(query, (display_name,))
    result = cursor.fetchone()
    return {"post_id": result[0]}

# Endpoint to get display name for users with specific names ordered by view count
@app.get("/v1/bird/codebase_community/user_display_name", summary="Get display name for users with specific names ordered by view count")
async def get_user_display_name(display_name1: str = Query(..., description="First display name"), display_name2: str = Query(..., description="Second display name")):
    query = f"SELECT T1.DisplayName FROM users AS T1 INNER JOIN postHistory AS T2 ON T1.Id = T2.UserId INNER JOIN posts AS T3 ON T2.PostId = T3.Id WHERE T1.DisplayName = ? OR T1.DisplayName = ? GROUP BY T1.DisplayName ORDER BY SUM(T3.ViewCount) DESC LIMIT 1"
    cursor.execute(query, (display_name1, display_name2))
    result = cursor.fetchone()
    return {"display_name": result[0]}

# Endpoint to get count of users with a specific display name and votes grouped by post ID and vote ID
@app.get("/v1/bird/codebase_community/user_post_vote_group_count", summary="Get count of users with a specific display name and votes grouped by post ID and vote ID")
async def get_user_post_vote_group_count(display_name: str = Query(..., description="Display name of the user")):
    query = f"SELECT COUNT(T1.Id) FROM users AS T1 INNER JOIN postHistory AS T2 ON T1.Id = T2.UserId INNER JOIN posts AS T3 ON T2.PostId = T3.Id INNER JOIN votes AS T4 ON T4.PostId = T3.Id WHERE T1.DisplayName = ? GROUP BY T2.PostId, T4.Id HAVING COUNT(T4.Id) > 4"
    cursor.execute(query, (display_name,))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get count of comments for a specific user with a score less than a certain value
@app.get("/v1/bird/codebase_community/user_comment_count", summary="Get count of comments for a specific user with a score less than a certain value")
async def get_user_comment_count(display_name: str = Query(..., description="Display name of the user"), score: int = Query(..., description="Score threshold")):
    query = f"SELECT COUNT(T3.Id) FROM users AS T1 INNER JOIN posts AS T2 ON T1.Id = T2.OwnerUserId INNER JOIN comments AS T3 ON T2.Id = T3.PostId WHERE T1.DisplayName = ? AND T3.Score < ?"
    cursor.execute(query, (display_name, score))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get tags for a specific user with no comments
@app.get("/v1/bird/codebase_community/user_tags", summary="Get tags for a specific user with no comments")
async def get_user_tags(display_name: str = Query(..., description="Display name of the user")):
    query = f"SELECT T3.Tags FROM users AS T1 INNER JOIN postHistory AS T2 ON T1.Id = T2.UserId INNER JOIN posts AS T3 ON T3.Id = T2.PostId WHERE T1.DisplayName = ? AND T3.CommentCount = 0"
    cursor.execute(query, (display_name,))
    result = cursor.fetchone()
    return {"tags": result[0]}

# Endpoint to get display name for users with a specific badge name
@app.get("/v1/bird/codebase_community/user_badge_display_name", summary="Get display name for users with a specific badge name")
async def get_user_badge_display_name(badge_name: str = Query(..., description="Name of the badge")):
    query = f"SELECT T1.DisplayName FROM users AS T1 INNER JOIN badges AS T2 ON T1.Id = T2.UserId WHERE T2.Name = ?"
    cursor.execute(query, (badge_name,))
    result = cursor.fetchone()
    return {"display_name": result[0]}

# Endpoint to get percentage of posts with a specific tag for a specific user
@app.get("/v1/bird/codebase_community/user_tag_percentage", summary="Get percentage of posts with a specific tag for a specific user")
async def get_user_tag_percentage(display_name: str = Query(..., description="Display name of the user"), tag_name: str = Query(..., description="Name of the tag")):
    query = f"SELECT CAST(SUM(IIF(T3.TagName = ?, 1, 0)) AS REAL) * 100 / COUNT(T1.Id) FROM users AS T1 INNER JOIN postHistory AS T2 ON T1.Id = T2.UserId INNER JOIN tags AS T3 ON T3.ExcerptPostId = T2.PostId WHERE T1.DisplayName = ?"
    cursor.execute(query, (tag_name, display_name))
    result = cursor.fetchone()
    return {"percentage": result[0]}

# Endpoint to get difference in view count between two users
@app.get("/v1/bird/codebase_community/user_view_count_diff", summary="Get difference in view count between two users")
async def get_user_view_count_diff(display_name1: str = Query(..., description="First display name"), display_name2: str = Query(..., description="Second display name")):
    query = f"SELECT SUM(IIF(T1.DisplayName = ?, T3.ViewCount, 0)) - SUM(IIF(T1.DisplayName = ?, T3.ViewCount, 0)) AS diff FROM users AS T1 INNER JOIN postHistory AS T2 ON T1.Id = T2.UserId INNER JOIN posts AS T3 ON T3.Id = T2.PostId"
    cursor.execute(query, (display_name1, display_name2))
    result = cursor.fetchone()
    return {"diff": result[0]}

# Endpoint to get count of badges with a specific name and year
@app.get("/v1/bird/codebase_community/badge_count", summary="Get count of badges with a specific name and year")
async def get_badge_count(badge_name: str = Query(..., description="Name of the badge"), year: str = Query(..., description="Year")):
    query = f"SELECT COUNT(Id) FROM badges WHERE Name = ? AND STRFTIME('%Y', Date) = ?"
    cursor.execute(query, (badge_name, year))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get count of post history on a specific date
@app.get("/v1/bird/codebase_community/post_history_count", summary="Get count of post history on a specific date")
async def get_post_history_count(date: str = Query(..., description="Date in YYYY-MM-DD format")):
    query = f"SELECT COUNT(id) FROM postHistory WHERE date(CreationDate) = ?"
    cursor.execute(query, (date,))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get users with the maximum views
@app.get("/v1/bird/codebase_community/max_views_users", summary="Get users with the maximum views")
async def get_max_views_users():
    query = f"SELECT DisplayName, Age FROM users WHERE Views = (SELECT MAX(Views) FROM users)"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"users": result}

# Endpoint to get last edit date and editor user ID for a specific post title
@app.get("/v1/bird/codebase_community/post_last_edit", summary="Get last edit date and editor user ID for a specific post title")
async def get_post_last_edit(title: str = Query(..., description="Title of the post")):
    query = f"SELECT LastEditDate, LastEditorUserId FROM posts WHERE Title = ?"
    cursor.execute(query, (title,))
    result = cursor.fetchone()
    return {"last_edit_date": result[0], "last_editor_user_id": result[1]}

# Endpoint to get count of comments for a specific user with a score less than a certain value
@app.get("/v1/bird/codebase_community/user_comment_count_by_score", summary="Get count of comments for a specific user with a score less than a certain value")
async def get_user_comment_count_by_score(user_id: int = Query(..., description="User ID"), score: int = Query(..., description="Score threshold")):
    query = f"SELECT COUNT(Id) FROM comments WHERE UserId = ? AND Score < ?"
    cursor.execute(query, (user_id, score))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get post title and user display name for posts with a score greater than a certain value
@app.get("/v1/bird/codebase_community/high_score_posts", summary="Get post title and user display name for posts with a score greater than a certain value")
async def get_high_score_posts(score: int = Query(..., description="Score threshold")):
    query = f"SELECT T1.Title, T2.UserDisplayName FROM posts AS T1 INNER JOIN comments AS T2 ON T2.PostId = T2.Id WHERE T1.Score > ?"
    cursor.execute(query, (score,))
    result = cursor.fetchall()
    return {"posts": result}

# Endpoint to get badge names for users in a specific location and year
@app.get("/v1/bird/codebase_community/user_badges_by_location_year", summary="Get badge names for users in a specific location and year")
async def get_user_badges_by_location_year(location: str = Query(..., description="Location of the user"), year: str = Query(..., description="Year")):
    query = f"SELECT T2.Name FROM users AS T1 INNER JOIN badges AS T2 ON T1.Id = T2.UserId WHERE STRFTIME('%Y', T2.Date) = ? AND T1.Location = ?"
    cursor.execute(query, (year, location))
    result = cursor.fetchall()
    return {"badges": result}

# Endpoint to get display name and website URL for users with a favorite count greater than a certain value
@app.get("/v1/bird/codebase_community/user_favorite_count", summary="Get display name and website URL for users with a favorite count greater than a certain value")
async def get_user_favorite_count(favorite_count: int = Query(..., description="Favorite count threshold")):
    query = f"SELECT T1.DisplayName, T1.WebsiteUrl FROM users AS T1 INNER JOIN posts AS T2 ON T1.Id = T2.OwnerUserId WHERE T2.FavoriteCount > ?"
    cursor.execute(query, (favorite_count,))
    result = cursor.fetchall()
    return {"users": result}

# Endpoint to get post ID and last edit date for a specific post title
@app.get("/v1/bird/codebase_community/post_edit_history", summary="Get post ID and last edit date for a specific post title")
async def get_post_edit_history(title: str = Query(..., description="Title of the post")):
    query = f"SELECT T1.Id, T2.LastEditDate FROM postHistory AS T1 INNER JOIN posts AS T2 ON T1.PostId = T2.Id WHERE T2.Title = ?"
    cursor.execute(query, (title,))
    result = cursor.fetchone()
    return {"post_id": result[0], "last_edit_date": result[1]}

# Endpoint to get last access date and location for users with a specific badge name
@app.get("/v1/bird/codebase_community/user_badge_access", summary="Get last access date and location for users with a specific badge name")
async def get_user_badge_access(badge_name: str = Query(..., description="Name of the badge")):
    query = f"SELECT T1.LastAccessDate, T1.Location FROM users AS T1 INNER JOIN badges AS T2 ON T1.Id = T2.UserId WHERE T2.Name = ?"
    cursor.execute(query, (badge_name,))
    result = cursor.fetchall()
    return {"users": result}
# Endpoint to get related post titles
@app.get("/v1/bird/codebase_community/related_post_titles", summary="Get related post titles for a given post title")
async def get_related_post_titles(post_title: str = Query(..., description="Title of the post")):
    query = """
    SELECT T3.Title
    FROM postLinks AS T1
    INNER JOIN posts AS T2 ON T1.PostId = T2.Id
    INNER JOIN posts AS T3 ON T1.RelatedPostId = T3.Id
    WHERE T2.Title = ?
    """
    cursor.execute(query, (post_title,))
    result = cursor.fetchall()
    return {"related_post_titles": result}

# Endpoint to get post ID and badge name for a given user display name and year
@app.get("/v1/bird/codebase_community/post_badge_info", summary="Get post ID and badge name for a given user display name and year")
async def get_post_badge_info(user_display_name: str = Query(..., description="User display name"), year: int = Query(..., description="Year")):
    query = """
    SELECT T1.PostId, T2.Name
    FROM postHistory AS T1
    INNER JOIN badges AS T2 ON T1.UserId = T2.UserId
    WHERE T1.UserDisplayName = ? AND STRFTIME('%Y', T1.CreationDate) = ? AND STRFTIME('%Y', T2.Date) = ?
    """
    cursor.execute(query, (user_display_name, str(year), str(year)))
    result = cursor.fetchall()
    return {"post_badge_info": result}

# Endpoint to get display name of the user with the highest view count
@app.get("/v1/bird/codebase_community/top_viewed_user", summary="Get display name of the user with the highest view count")
async def get_top_viewed_user():
    query = """
    SELECT DisplayName
    FROM users
    WHERE Id = (SELECT OwnerUserId FROM posts ORDER BY ViewCount DESC LIMIT 1)
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"top_viewed_user": result}

# Endpoint to get user details for a given tag name
@app.get("/v1/bird/codebase_community/user_details_by_tag", summary="Get user details for a given tag name")
async def get_user_details_by_tag(tag_name: str = Query(..., description="Tag name")):
    query = """
    SELECT T3.DisplayName, T3.Location
    FROM tags AS T1
    INNER JOIN posts AS T2 ON T1.ExcerptPostId = T2.Id
    INNER JOIN users AS T3 ON T3.Id = T2.OwnerUserId
    WHERE T1.TagName = ?
    """
    cursor.execute(query, (tag_name,))
    result = cursor.fetchall()
    return {"user_details": result}

# Endpoint to get related post titles and link type for a given post title
@app.get("/v1/bird/codebase_community/related_post_info", summary="Get related post titles and link type for a given post title")
async def get_related_post_info(post_title: str = Query(..., description="Title of the post")):
    query = """
    SELECT T3.Title, T2.LinkTypeId
    FROM posts AS T1
    INNER JOIN postLinks AS T2 ON T1.Id = T2.PostId
    INNER JOIN posts AS T3 ON T2.RelatedPostId = T3.Id
    WHERE T1.Title = ?
    """
    cursor.execute(query, (post_title,))
    result = cursor.fetchall()
    return {"related_post_info": result}

# Endpoint to get display name of the user with the highest score for a given parent ID
@app.get("/v1/bird/codebase_community/top_scored_user", summary="Get display name of the user with the highest score for a given parent ID")
async def get_top_scored_user():
    query = """
    SELECT DisplayName
    FROM users
    WHERE Id = (SELECT OwnerUserId FROM posts WHERE ParentId IS NOT NULL ORDER BY Score DESC LIMIT 1)
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"top_scored_user": result}

# Endpoint to get user details with the highest bounty amount for a given vote type ID
@app.get("/v1/bird/codebase_community/top_bounty_user", summary="Get user details with the highest bounty amount for a given vote type ID")
async def get_top_bounty_user(vote_type_id: int = Query(..., description="Vote type ID")):
    query = """
    SELECT DisplayName, WebsiteUrl
    FROM users
    WHERE Id = (SELECT UserId FROM votes WHERE VoteTypeId = ? ORDER BY BountyAmount DESC LIMIT 1)
    """
    cursor.execute(query, (vote_type_id,))
    result = cursor.fetchone()
    return {"top_bounty_user": result}

# Endpoint to get top viewed posts
@app.get("/v1/bird/codebase_community/top_viewed_posts", summary="Get top viewed posts")
async def get_top_viewed_posts(limit: int = Query(5, description="Number of top viewed posts to retrieve")):
    query = """
    SELECT Title
    FROM posts
    ORDER BY ViewCount DESC
    LIMIT ?
    """
    cursor.execute(query, (limit,))
    result = cursor.fetchall()
    return {"top_viewed_posts": result}

# Endpoint to get count of tags within a given range
@app.get("/v1/bird/codebase_community/tag_count", summary="Get count of tags within a given range")
async def get_tag_count(min_count: int = Query(..., description="Minimum count"), max_count: int = Query(..., description="Maximum count")):
    query = """
    SELECT COUNT(Id)
    FROM tags
    WHERE Count BETWEEN ? AND ?
    """
    cursor.execute(query, (min_count, max_count))
    result = cursor.fetchone()
    return {"tag_count": result}

# Endpoint to get user ID with the maximum favorite count
@app.get("/v1/bird/codebase_community/max_favorite_user", summary="Get user ID with the maximum favorite count")
async def get_max_favorite_user():
    query = """
    SELECT OwnerUserId
    FROM posts
    WHERE FavoriteCount = (SELECT MAX(FavoriteCount) FROM posts)
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"max_favorite_user": result}

# Endpoint to get age of the user with the maximum reputation
@app.get("/v1/bird/codebase_community/max_reputation_age", summary="Get age of the user with the maximum reputation")
async def get_max_reputation_age():
    query = """
    SELECT Age
    FROM users
    WHERE Reputation = (SELECT MAX(Reputation) FROM users)
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"max_reputation_age": result}

# Endpoint to get count of posts with a given bounty amount and year
@app.get("/v1/bird/codebase_community/post_count_by_bounty", summary="Get count of posts with a given bounty amount and year")
async def get_post_count_by_bounty(bounty_amount: int = Query(..., description="Bounty amount"), year: int = Query(..., description="Year")):
    query = """
    SELECT COUNT(T1.Id)
    FROM posts AS T1
    INNER JOIN votes AS T2 ON T1.Id = T2.PostId
    WHERE T2.BountyAmount = ? AND STRFTIME('%Y', T2.CreationDate) = ?
    """
    cursor.execute(query, (bounty_amount, str(year)))
    result = cursor.fetchone()
    return {"post_count": result}

# Endpoint to get user ID with the minimum age
@app.get("/v1/bird/codebase_community/min_age_user", summary="Get user ID with the minimum age")
async def get_min_age_user():
    query = """
    SELECT Id
    FROM users
    WHERE Age = (SELECT MIN(Age) FROM users)
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"min_age_user": result}

# Endpoint to get sum of scores for a given last activity date
@app.get("/v1/bird/codebase_community/sum_scores_by_date", summary="Get sum of scores for a given last activity date")
async def get_sum_scores_by_date(last_activity_date: str = Query(..., description="Last activity date (YYYY-MM-DD)")):
    query = """
    SELECT SUM(Score)
    FROM posts
    WHERE LastActivityDate LIKE ?
    """
    cursor.execute(query, (last_activity_date + '%',))
    result = cursor.fetchone()
    return {"sum_scores": result}

# Endpoint to get average post links count for a given year and maximum answer count
@app.get("/v1/bird/codebase_community/avg_post_links_count", summary="Get average post links count for a given year and maximum answer count")
async def get_avg_post_links_count(year: int = Query(..., description="Year"), max_answer_count: int = Query(..., description="Maximum answer count")):
    query = """
    SELECT CAST(COUNT(T1.Id) AS REAL) / 12
    FROM postLinks AS T1
    INNER JOIN posts AS T2 ON T1.PostId = T2.Id
    WHERE T2.AnswerCount <= ? AND STRFTIME('%Y', T1.CreationDate) = ?
    """
    cursor.execute(query, (max_answer_count, str(year)))
    result = cursor.fetchone()
    return {"avg_post_links_count": result}

# Endpoint to get post ID with the highest favorite count for a given user ID
@app.get("/v1/bird/codebase_community/top_favorite_post_by_user", summary="Get post ID with the highest favorite count for a given user ID")
async def get_top_favorite_post_by_user(user_id: int = Query(..., description="User ID")):
    query = """
    SELECT T2.Id
    FROM votes AS T1
    INNER JOIN posts AS T2 ON T1.PostId = T2.Id
    WHERE T1.UserId = ?
    ORDER BY T2.FavoriteCount DESC
    LIMIT 1
    """
    cursor.execute(query, (user_id,))
    result = cursor.fetchone()
    return {"top_favorite_post": result}

# Endpoint to get post title ordered by creation date
@app.get("/v1/bird/codebase_community/post_title_by_creation_date", summary="Get post title ordered by creation date")
async def get_post_title_by_creation_date():
    query = """
    SELECT T1.Title
    FROM posts AS T1
    INNER JOIN postLinks AS T2 ON T2.PostId = T1.Id
    ORDER BY T1.CreationDate
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"post_title": result}

# Endpoint to get display name with the highest badge count
@app.get("/v1/bird/codebase_community/top_badge_user", summary="Get display name with the highest badge count")
async def get_top_badge_user():
    query = """
    SELECT T1.DisplayName
    FROM users AS T1
    INNER JOIN badges AS T2 ON T1.Id = T2.UserId
    GROUP BY T1.DisplayName
    ORDER BY COUNT(T1.Id) DESC
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"top_badge_user": result}

# Endpoint to get creation date of the first vote for a given user display name
@app.get("/v1/bird/codebase_community/first_vote_creation_date", summary="Get creation date of the first vote for a given user display name")
async def get_first_vote_creation_date(display_name: str = Query(..., description="User display name")):
    query = """
    SELECT T2.CreationDate
    FROM users AS T1
    INNER JOIN votes AS T2 ON T1.Id = T2.UserId
    WHERE T1.DisplayName = ?
    ORDER BY T2.CreationDate
    LIMIT 1
    """
    cursor.execute(query, (display_name,))
    result = cursor.fetchone()
    return {"first_vote_creation_date": result}

# Endpoint to get creation date of the first post for users with non-null age
@app.get("/v1/bird/codebase_community/first_post_creation_date", summary="Get creation date of the first post for users with non-null age")
async def get_first_post_creation_date():
    query = """
    SELECT T2.CreationDate
    FROM users AS T1
    INNER JOIN posts AS T2 ON T1.Id = T2.OwnerUserId
    WHERE T1.Age IS NOT NULL
    ORDER BY T1.Age, T2.CreationDate
    LIMIT 1
    """
    cursor.execute(query)
    result = cursor.fetchone()
    return {"first_post_creation_date": result}
# Endpoint to get display name of users with a specific badge
@app.get("/v1/bird/codebase_community/users_with_badge", summary="Get users with a specific badge")
async def get_users_with_badge(badge_name: str = Query(..., description="Name of the badge")):
    query = f"SELECT T1.DisplayName FROM users AS T1 INNER JOIN badges AS T2 ON T1.Id = T2.UserId WHERE T2.Name = ? ORDER BY T2.Date LIMIT 1"
    cursor.execute(query, (badge_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get count of users with specific location and favorite count
@app.get("/v1/bird/codebase_community/user_post_count", summary="Get count of users with specific location and favorite count")
async def get_user_post_count(location: str = Query(..., description="Location of the user"), favorite_count: int = Query(..., description="Minimum favorite count")):
    query = f"SELECT COUNT(T1.Id) FROM users AS T1 INNER JOIN posts AS T2 ON T1.Id = T2.OwnerUserId WHERE T1.Location = ? AND T2.FavoriteCount >= ?"
    cursor.execute(query, (location, favorite_count))
    result = cursor.fetchall()
    return result

# Endpoint to get average post id of users with maximum age
@app.get("/v1/bird/codebase_community/avg_post_id_max_age", summary="Get average post id of users with maximum age")
async def get_avg_post_id_max_age():
    query = f"SELECT AVG(PostId) FROM votes WHERE UserId IN ( SELECT Id FROM users WHERE Age = ( SELECT MAX(Age) FROM users ) )"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get user with maximum reputation
@app.get("/v1/bird/codebase_community/user_max_reputation", summary="Get user with maximum reputation")
async def get_user_max_reputation():
    query = f"SELECT DisplayName FROM users WHERE Reputation = ( SELECT MAX(Reputation) FROM users )"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get count of users with specific reputation and views
@app.get("/v1/bird/codebase_community/user_count_reputation_views", summary="Get count of users with specific reputation and views")
async def get_user_count_reputation_views(reputation: int = Query(..., description="Minimum reputation"), views: int = Query(..., description="Minimum views")):
    query = f"SELECT COUNT(id) FROM users WHERE Reputation > ? AND Views > ?"
    cursor.execute(query, (reputation, views))
    result = cursor.fetchall()
    return result

# Endpoint to get users within a specific age range
@app.get("/v1/bird/codebase_community/users_age_range", summary="Get users within a specific age range")
async def get_users_age_range(min_age: int = Query(..., description="Minimum age"), max_age: int = Query(..., description="Maximum age")):
    query = f"SELECT DisplayName FROM users WHERE Age BETWEEN ? AND ?"
    cursor.execute(query, (min_age, max_age))
    result = cursor.fetchall()
    return result

# Endpoint to get count of posts by a specific user in a specific year
@app.get("/v1/bird/codebase_community/post_count_user_year", summary="Get count of posts by a specific user in a specific year")
async def get_post_count_user_year(year: str = Query(..., description="Year of the post"), display_name: str = Query(..., description="Display name of the user")):
    query = f"SELECT COUNT(T1.Id) FROM users AS T1 INNER JOIN posts AS T2 ON T1.Id = T2.OwnerUserId WHERE STRFTIME('%Y', T2.CreaionDate) = ? AND T1.DisplayName = ?"
    cursor.execute(query, (year, display_name))
    result = cursor.fetchall()
    return result

# Endpoint to get top post by a specific user
@app.get("/v1/bird/codebase_community/top_post_user", summary="Get top post by a specific user")
async def get_top_post_user(display_name: str = Query(..., description="Display name of the user")):
    query = f"SELECT T2.Id, T2.Title FROM users AS T1 INNER JOIN posts AS T2 ON T1.Id = T2.OwnerUserId WHERE T1.DisplayName = ? ORDER BY T2.ViewCount DESC LIMIT 1"
    cursor.execute(query, (display_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get top post by score
@app.get("/v1/bird/codebase_community/top_post_score", summary="Get top post by score")
async def get_top_post_score():
    query = f"SELECT T1.Id, T2.Title FROM users AS T1 INNER JOIN posts AS T2 ON T1.Id = T2.OwnerUserId ORDER BY T2.Score DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get average score of posts by a specific user
@app.get("/v1/bird/codebase_community/avg_post_score_user", summary="Get average score of posts by a specific user")
async def get_avg_post_score_user(display_name: str = Query(..., description="Display name of the user")):
    query = f"SELECT AVG(T2.Score) FROM users AS T1 INNER JOIN posts AS T2 ON T1.Id = T2.OwnerUserId WHERE T1.DisplayName = ?"
    cursor.execute(query, (display_name,))
    result = cursor.fetchall()
    return result

# Endpoint to get users with posts in a specific year and view count
@app.get("/v1/bird/codebase_community/users_posts_year_viewcount", summary="Get users with posts in a specific year and view count")
async def get_users_posts_year_viewcount(year: str = Query(..., description="Year of the post"), view_count: int = Query(..., description="Minimum view count")):
    query = f"SELECT T1.DisplayName FROM users AS T1 INNER JOIN posts AS T2 ON T1.Id = T2.OwnerUserId WHERE STRFTIME('%Y', T2.CreaionDate) = ? AND T2.ViewCount > ?"
    cursor.execute(query, (year, view_count))
    result = cursor.fetchall()
    return result

# Endpoint to get top user by favorite count in a specific year
@app.get("/v1/bird/codebase_community/top_user_favorite_count_year", summary="Get top user by favorite count in a specific year")
async def get_top_user_favorite_count_year(year: str = Query(..., description="Year of the user creation")):
    query = f"SELECT T2.OwnerUserId, T1.DisplayName FROM users AS T1 INNER JOIN posts AS T2 ON T1.Id = T2.OwnerUserId WHERE STRFTIME('%Y', T1.CreationDate) = ? ORDER BY T2.FavoriteCount DESC LIMIT 1"
    cursor.execute(query, (year,))
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of users with reputation and posts in a specific year
@app.get("/v1/bird/codebase_community/percentage_users_reputation_posts_year", summary="Get percentage of users with reputation and posts in a specific year")
async def get_percentage_users_reputation_posts_year(year: str = Query(..., description="Year of the post"), reputation: int = Query(..., description="Minimum reputation")):
    query = f"SELECT CAST(SUM(IIF(STRFTIME('%Y', T2.CreaionDate) = ? AND T1.Reputation > ?, 1, 0)) AS REAL) * 100 / COUNT(T1.Id) FROM users AS T1 INNER JOIN posts AS T2 ON T1.Id = T2.OwnerUserId"
    cursor.execute(query, (year, reputation))
    result = cursor.fetchall()
    return result

# Endpoint to get percentage of users within a specific age range
@app.get("/v1/bird/codebase_community/percentage_users_age_range", summary="Get percentage of users within a specific age range")
async def get_percentage_users_age_range(min_age: int = Query(..., description="Minimum age"), max_age: int = Query(..., description="Maximum age")):
    query = f"SELECT CAST(SUM(IIF(Age BETWEEN ? AND ?, 1, 0)) AS REAL) * 100 / COUNT(Id) FROM users"
    cursor.execute(query, (min_age, max_age))
    result = cursor.fetchall()
    return result

# Endpoint to get post history with specific text
@app.get("/v1/bird/codebase_community/post_history_text", summary="Get post history with specific text")
async def get_post_history_text(text: str = Query(..., description="Text in post history")):
    query = f"SELECT T2.ViewCount, T3.DisplayName FROM postHistory AS T1 INNER JOIN posts AS T2 ON T1.PostId = T2.Id INNER JOIN users AS T3 ON T2.LastEditorUserId = T3.Id WHERE T1.Text = ?"
    cursor.execute(query, (text,))
    result = cursor.fetchall()
    return result

# Endpoint to get posts with view count greater than average
@app.get("/v1/bird/codebase_community/posts_viewcount_greater_avg", summary="Get posts with view count greater than average")
async def get_posts_viewcount_greater_avg():
    query = f"SELECT Id FROM posts WHERE ViewCount > ( SELECT AVG(ViewCount) FROM posts )"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get top post by comment count
@app.get("/v1/bird/codebase_community/top_post_comment_count", summary="Get top post by comment count")
async def get_top_post_comment_count():
    query = f"SELECT COUNT(T2.Id) FROM posts AS T1 INNER JOIN comments AS T2 ON T1.Id = T2.PostId GROUP BY T1.Id ORDER BY SUM(T1.Score) DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchall()
    return result

# Endpoint to get posts with specific view count and comment count
@app.get("/v1/bird/codebase_community/posts_viewcount_commentcount", summary="Get posts with specific view count and comment count")
async def get_posts_viewcount_commentcount(view_count: int = Query(..., description="Minimum view count"), comment_count: int = Query(..., description="Comment count")):
    query = f"SELECT COUNT(Id) FROM posts WHERE ViewCount > ? AND CommentCount = ?"
    cursor.execute(query, (view_count, comment_count))
    result = cursor.fetchall()
    return result

# Endpoint to get user details for a specific post
@app.get("/v1/bird/codebase_community/user_details_post", summary="Get user details for a specific post")
async def get_user_details_post(post_id: int = Query(..., description="ID of the post")):
    query = f"SELECT T2.DisplayName, T2.Location FROM posts AS T1 INNER JOIN users AS T2 ON T1.OwnerUserId = T2.Id WHERE T1.Id = ? ORDER BY T1.LastEditDate DESC LIMIT 1"
    cursor.execute(query, (post_id,))
    result = cursor.fetchall()
    return result

# Endpoint to get badge of a specific user
@app.get("/v1/bird/codebase_community/user_badge", summary="Get badge of a specific user")
async def get_user_badge(display_name: str = Query(..., description="Display name of the user")):
    query = f"SELECT T1.Name FROM badges AS T1 INNER JOIN users AS T2 ON T1.UserId = T2.Id WHERE T2.DisplayName = ? ORDER BY T1.Date DESC LIMIT 1"
    cursor.execute(query, (display_name,))
    result = cursor.fetchall()
    return result
# Endpoint to get count of users within a specific age range and upvotes
@app.get("/v1/bird/codebase_community/user_count", summary="Get count of users within a specific age range and upvotes")
async def get_user_count(min_age: int = Query(..., description="Minimum age"),
                         max_age: int = Query(..., description="Maximum age"),
                         min_upvotes: int = Query(..., description="Minimum upvotes")):
    query = f"SELECT COUNT(Id) FROM users WHERE Age BETWEEN {min_age} AND {max_age} AND UpVotes > {min_upvotes}"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"user_count": result[0]}

# Endpoint to get date difference between badges and user creation
@app.get("/v1/bird/codebase_community/date_difference", summary="Get date difference between badges and user creation")
async def get_date_difference(display_name: str = Query(..., description="Display name of the user")):
    query = f"SELECT T1.Date - T2.CreationDate FROM badges AS T1 INNER JOIN users AS T2 ON T1.UserId = T2.Id WHERE T2.DisplayName = '{display_name}'"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"date_difference": result[0]}

# Endpoint to get count of posts with comments ordered by user creation date
@app.get("/v1/bird/codebase_community/post_count", summary="Get count of posts with comments ordered by user creation date")
async def get_post_count():
    query = "SELECT COUNT(T2.Id) FROM users AS T1 INNER JOIN posts AS T2 ON T1.Id = T2.OwnerUserId INNER JOIN comments AS T3 ON T3.PostId = T2.Id ORDER BY T1.CreationDate DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"post_count": result[0]}

# Endpoint to get comments and user display name for a specific post title
@app.get("/v1/bird/codebase_community/comments_by_post_title", summary="Get comments and user display name for a specific post title")
async def get_comments_by_post_title(post_title: str = Query(..., description="Title of the post")):
    query = f"SELECT T3.Text, T1.DisplayName FROM users AS T1 INNER JOIN posts AS T2 ON T1.Id = T2.OwnerUserId INNER JOIN comments AS T3 ON T2.Id = T3.PostId WHERE T2.Title = '{post_title}' ORDER BY T1.CreationDate DESC LIMIT 10"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"comments": result}

# Endpoint to get count of badges with a specific name
@app.get("/v1/bird/codebase_community/badge_count", summary="Get count of badges with a specific name")
async def get_badge_count(badge_name: str = Query(..., description="Name of the badge")):
    query = f"SELECT COUNT(id) FROM badges WHERE `Name` = '{badge_name}'"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"badge_count": result[0]}

# Endpoint to get count of tags with a specific tag name
@app.get("/v1/bird/codebase_community/tag_count", summary="Get count of tags with a specific tag name")
async def get_tag_count(tag_name: str = Query(..., description="Name of the tag")):
    query = f"SELECT COUNT(Id) FROM tags WHERE TagName = '{tag_name}'"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"tag_count": result[0]}

# Endpoint to get reputation and views for a specific user
@app.get("/v1/bird/codebase_community/user_reputation_views", summary="Get reputation and views for a specific user")
async def get_user_reputation_views(display_name: str = Query(..., description="Display name of the user")):
    query = f"SELECT Reputation, Views FROM users WHERE DisplayName = '{display_name}'"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"reputation": result[0], "views": result[1]}

# Endpoint to get comment and answer count for a specific post title
@app.get("/v1/bird/codebase_community/post_comment_answer_count", summary="Get comment and answer count for a specific post title")
async def get_post_comment_answer_count(post_title: str = Query(..., description="Title of the post")):
    query = f"SELECT CommentCount, AnswerCount FROM posts WHERE Title = '{post_title}'"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"comment_count": result[0], "answer_count": result[1]}

# Endpoint to get creation date for a specific user
@app.get("/v1/bird/codebase_community/user_creation_date", summary="Get creation date for a specific user")
async def get_user_creation_date(display_name: str = Query(..., description="Display name of the user")):
    query = f"SELECT CreationDate FROM users WHERE DisplayName = '{display_name}'"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"creation_date": result[0]}

# Endpoint to get count of votes with a specific bounty amount
@app.get("/v1/bird/codebase_community/vote_count", summary="Get count of votes with a specific bounty amount")
async def get_vote_count(min_bounty_amount: int = Query(..., description="Minimum bounty amount")):
    query = f"SELECT COUNT(id) FROM votes WHERE BountyAmount >= {min_bounty_amount}"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"vote_count": result[0]}

# Endpoint to get percentage of posts with a score greater than a specific value
@app.get("/v1/bird/codebase_community/post_score_percentage", summary="Get percentage of posts with a score greater than a specific value")
async def get_post_score_percentage(min_score: int = Query(..., description="Minimum score")):
    query = f"SELECT CAST(SUM(CASE WHEN T2.Score > {min_score} THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(T1.Id) FROM users T1 INNER JOIN posts T2 ON T1.Id = T2.OwnerUserId INNER JOIN ( SELECT MAX(Reputation) AS max_reputation FROM users ) T3 ON T1.Reputation = T3.max_reputation"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"score_percentage": result[0]}

# Endpoint to get count of posts with a score less than a specific value
@app.get("/v1/bird/codebase_community/low_score_post_count", summary="Get count of posts with a score less than a specific value")
async def get_low_score_post_count(max_score: int = Query(..., description="Maximum score")):
    query = f"SELECT COUNT(id) FROM posts WHERE Score < {max_score}"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"low_score_post_count": result[0]}

# Endpoint to get count of tags with a count less than or equal to a specific value and id less than a specific value
@app.get("/v1/bird/codebase_community/tag_count_by_id", summary="Get count of tags with a count less than or equal to a specific value and id less than a specific value")
async def get_tag_count_by_id(max_count: int = Query(..., description="Maximum count"), max_id: int = Query(..., description="Maximum id")):
    query = f"SELECT COUNT(id) FROM tags WHERE Count <= {max_count} AND Id < {max_id}"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"tag_count": result[0]}

# Endpoint to get excerpt post id and wiki post id for a specific tag name
@app.get("/v1/bird/codebase_community/tag_post_ids", summary="Get excerpt post id and wiki post id for a specific tag name")
async def get_tag_post_ids(tag_name: str = Query(..., description="Name of the tag")):
    query = f"SELECT ExcerptPostId, WikiPostId FROM tags WHERE TagName = '{tag_name}'"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"excerpt_post_id": result[0], "wiki_post_id": result[1]}

# Endpoint to get reputation and upvotes for a specific comment text
@app.get("/v1/bird/codebase_community/comment_reputation_upvotes", summary="Get reputation and upvotes for a specific comment text")
async def get_comment_reputation_upvotes(comment_text: str = Query(..., description="Text of the comment")):
    query = f"SELECT T2.Reputation, T2.UpVotes FROM comments AS T1 INNER JOIN users AS T2 ON T1.UserId = T2.Id WHERE T1.Text = '{comment_text}'"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"reputation": result[0], "upvotes": result[1]}

# Endpoint to get comments for posts with a specific title pattern
@app.get("/v1/bird/codebase_community/comments_by_post_title_pattern", summary="Get comments for posts with a specific title pattern")
async def get_comments_by_post_title_pattern(title_pattern: str = Query(..., description="Pattern in the post title")):
    query = f"SELECT T1.Text FROM comments AS T1 INNER JOIN posts AS T2 ON T1.PostId = T2.Id WHERE T2.Title LIKE '%{title_pattern}%'"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"comments": result}

# Endpoint to get comment text for posts with a specific view count range
@app.get("/v1/bird/codebase_community/comment_text_by_view_count", summary="Get comment text for posts with a specific view count range")
async def get_comment_text_by_view_count(min_view_count: int = Query(..., description="Minimum view count"), max_view_count: int = Query(..., description="Maximum view count")):
    query = f"SELECT Text FROM comments WHERE PostId IN ( SELECT Id FROM posts WHERE ViewCount BETWEEN {min_view_count} AND {max_view_count} ) ORDER BY Score DESC LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"comment_text": result[0]}

# Endpoint to get creation date and age for comments with a specific text pattern
@app.get("/v1/bird/codebase_community/comment_creation_date_age", summary="Get creation date and age for comments with a specific text pattern")
async def get_comment_creation_date_age(text_pattern: str = Query(..., description="Pattern in the comment text")):
    query = f"SELECT T2.CreationDate, T2.Age FROM comments AS T1 INNER JOIN users AS T2 ON T1.UserId = T2.Id WHERE T1.text LIKE '%{text_pattern}%'"
    cursor.execute(query)
    result = cursor.fetchall()
    return {"creation_date_age": result}

# Endpoint to get count of comments for posts with a specific view count and score
@app.get("/v1/bird/codebase_community/comment_count_by_view_score", summary="Get count of comments for posts with a specific view count and score")
async def get_comment_count_by_view_score(max_view_count: int = Query(..., description="Maximum view count"), score: int = Query(..., description="Score")):
    query = f"SELECT COUNT(T1.Id) FROM comments AS T1 INNER JOIN posts AS T2 ON T1.PostId = T2.Id WHERE T2.ViewCount < {max_view_count} AND T2.Score = {score}"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"comment_count": result[0]}

# Endpoint to get count of comments for posts with a specific comment count and score
@app.get("/v1/bird/codebase_community/comment_count_by_comment_score", summary="Get count of comments for posts with a specific comment count and score")
async def get_comment_count_by_comment_score(comment_count: int = Query(..., description="Comment count"), score: int = Query(..., description="Score")):
    query = f"SELECT COUNT(T1.id) FROM comments AS T1 INNER JOIN posts AS T2 ON T1.PostId = T2.Id WHERE T2.CommentCount = {comment_count} AND T2.Score = {score}"
    cursor.execute(query)
    result = cursor.fetchone()
    return {"comment_count": result[0]}

# Endpoint to get count of distinct comments with specific score and user age
@app.get("/v1/bird/codebase_community/count_distinct_comments", summary="Get count of distinct comments with specific score and user age")
async def get_count_distinct_comments(score: int = Query(..., description="Score of the comment"), age: int = Query(..., description="Age of the user")):
    query = """
    SELECT COUNT(DISTINCT T1.id)
    FROM comments AS T1
    INNER JOIN users AS T2 ON T1.UserId = T2.Id
    WHERE T1.Score = ? AND T2.Age = ?
    """
    cursor.execute(query, (score, age))
    result = cursor.fetchone()
    return {"count": result[0]}

# Endpoint to get post ID and comment text for a specific post title
@app.get("/v1/bird/codebase_community/post_comments", summary="Get post ID and comment text for a specific post title")
async def get_post_comments(post_title: str = Query(..., description="Title of the post")):
    query = """
    SELECT T2.Id, T1.Text
    FROM comments AS T1
    INNER JOIN posts AS T2 ON T1.PostId = T2.Id
    WHERE T2.Title = ?
    """
    cursor.execute(query, (post_title,))
    result = cursor.fetchall()
    return {"comments": result}

# Endpoint to get user upvotes for a specific comment text
@app.get("/v1/bird/codebase_community/user_upvotes", summary="Get user upvotes for a specific comment text")
async def get_user_upvotes(comment_text: str = Query(..., description="Text of the comment")):
    query = """
    SELECT T2.UpVotes
    FROM comments AS T1
    INNER JOIN users AS T2 ON T1.UserId = T2.Id
    WHERE T1.Text = ?
    """
    cursor.execute(query, (comment_text,))
    result = cursor.fetchall()
    return {"upvotes": result}

# Endpoint to get comment text for a specific user display name
@app.get("/v1/bird/codebase_community/user_comments", summary="Get comment text for a specific user display name")
async def get_user_comments(display_name: str = Query(..., description="Display name of the user")):
    query = """
    SELECT T1.Text
    FROM comments AS T1
    INNER JOIN users AS T2 ON T1.UserId = T2.Id
    WHERE T2.DisplayName = ?
    """
    cursor.execute(query, (display_name,))
    result = cursor.fetchall()
    return {"comments": result}

# Endpoint to get user display names for comments with specific score range and downvotes
@app.get("/v1/bird/codebase_community/user_display_names", summary="Get user display names for comments with specific score range and downvotes")
async def get_user_display_names(min_score: int = Query(..., description="Minimum score of the comment"), max_score: int = Query(..., description="Maximum score of the comment"), downvotes: int = Query(..., description="Downvotes of the user")):
    query = """
    SELECT T2.DisplayName
    FROM comments AS T1
    INNER JOIN users AS T2 ON T1.UserId = T2.Id
    WHERE T1.Score BETWEEN ? AND ? AND T2.DownVotes = ?
    """
    cursor.execute(query, (min_score, max_score, downvotes))
    result = cursor.fetchall()
    return {"display_names": result}

# Endpoint to get percentage of users with zero upvotes for comments with specific score range
@app.get("/v1/bird/codebase_community/percentage_zero_upvotes", summary="Get percentage of users with zero upvotes for comments with specific score range")
async def get_percentage_zero_upvotes(min_score: int = Query(..., description="Minimum score of the comment"), max_score: int = Query(..., description="Maximum score of the comment")):
    query = """
    SELECT CAST(SUM(IIF(T1.UpVotes = 0, 1, 0)) AS REAL) * 100/ COUNT(T1.Id) AS per
    FROM users AS T1
    INNER JOIN comments AS T2 ON T1.Id = T2.UserId
    WHERE T2.Score BETWEEN ? AND ?
    """
    cursor.execute(query, (min_score, max_score))
    result = cursor.fetchone()
    return {"percentage": result[0]}
